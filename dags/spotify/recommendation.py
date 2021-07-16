import io
import re

import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from spotify.etl import get_new_access_token_from_refresh_token, get_track_details


def create_necessary_outputs(user_spotify: pd.DataFrame):
    """Return user_spotify in all_spotify"""
    user_spotify["date_added"] = pd.to_datetime(user_spotify["date_added"])
    # user_spotify = user_spotify[
    #     user_spotify["id"].isin(all_spotify["id"].values)
    # ].sort_values("date_added", ascending=False)
    return user_spotify


def generate_playlist_feature(complete_feature_set, playlist_df, weight_factor):
    """
    Summarize a user's playlist into a single vector

    Parameters:
        complete_feature_set (pandas dataframe): Dataframe which includes all of the features for the spotify songs
        playlist_df (pandas dataframe): playlist dataframe
        weight_factor (float): float value that represents the recency bias. The larger the recency bias, the most priority recent songs get. Value should be close to 1.

    Returns:
        playlist_feature_set_weighted_final (pandas series): single feature that summarizes the playlist
        complete_feature_set_nonplaylist (pandas dataframe):
    """

    # complete_feature_set_playlist = complete_feature_set[
    #     complete_feature_set["id"].isin(playlist_df["id"].values)
    # ]  # .drop('id', axis = 1).mean(axis =0)
    complete_feature_set_playlist = complete_feature_set.merge(
        playlist_df[["id", "date_added"]], on="id", how="inner"
    )
    complete_feature_set_nonplaylist = complete_feature_set[
        ~complete_feature_set["id"].isin(playlist_df["id"].values)
    ]  # .drop('id', axis = 1)

    playlist_feature_set = complete_feature_set_playlist.sort_values(
        "date_added", ascending=False
    )

    if playlist_feature_set.empty:
        return pd.DataFrame([]), pd.DataFrame([])

    most_recent_date = playlist_feature_set.iloc[0, -1]

    for ix, row in playlist_feature_set.iterrows():
        playlist_feature_set.loc[ix, "months_from_recent"] = int(
            (most_recent_date.to_pydatetime() - row.iloc[-1].to_pydatetime()).days / 30
        )

    playlist_feature_set["weight"] = playlist_feature_set["months_from_recent"].apply(
        lambda x: weight_factor ** (-x)
    )

    playlist_feature_set_weighted = playlist_feature_set.copy()
    # print(playlist_feature_set_weighted.iloc[:,:-4].columns)
    playlist_feature_set_weighted.update(
        playlist_feature_set_weighted.iloc[:, :-4].mul(
            playlist_feature_set_weighted.weight, 0
        )
    )
    playlist_feature_set_weighted_final = playlist_feature_set_weighted.iloc[:, :-4]
    # playlist_feature_set_weighted_final['id'] = playlist_feature_set['id']

    return (
        playlist_feature_set_weighted_final.sum(axis=0),
        complete_feature_set_nonplaylist,
    )


def generate_recomendation(df, features, nonlist_features):
    """
    Pull songs from a specific playlist.

    Parameters:
        df (pandas dataframe): spotify dataframe
        features (pandas series): summarized playlist feature
        nonplaylist_features (pandas dataframe): feature set of songs that are not in the selected playlist

    Returns:
        non_listed_df: All Top recommendations for that playlist
    """

    non_listed_df = df[df["id"].isin(nonlist_features["id"].values)]
    non_listed_df.loc[:, "sim"] = cosine_similarity(
        nonlist_features.drop("id", axis=1).values, features.values.reshape(1, -1)
    )[:, 0]
    non_listed_df = non_listed_df.sort_values("sim", ascending=False).head(40)
    return non_listed_df


def get_recommendations(
    complete_feature_set,
    all_spotify,
    file_name,
    refresh_token,
    bucket_name="spotify-users",
):
    from spotify.utils import get_minio_client

    print("Processing recomendations for ", file_name)
    minio_client = get_minio_client()

    user_spotify_object = minio_client.get_object(
        bucket_name=bucket_name, object_name=file_name
    )
    user_spotify = pd.read_csv(user_spotify_object)
    user_spotify = create_necessary_outputs(user_spotify)

    (
        complete_feature_set_vector,
        complete_feature_set_vector_nonlisted,
    ) = generate_playlist_feature(complete_feature_set, user_spotify, 1.09)

    if complete_feature_set_vector.empty or complete_feature_set_vector_nonlisted.empty:
        print("No recommendations for ", file_name)
        return

    n = 1000
    nonlist_features_bucket_df = [
        complete_feature_set_vector_nonlisted[i : i + n]
        for i in range(0, complete_feature_set_vector_nonlisted.shape[0], n)
    ]

    all_recommendation = []
    for lst in nonlist_features_bucket_df:
        recommendation = generate_recomendation(
            df=all_spotify,
            features=complete_feature_set_vector,
            nonlist_features=lst,
        )
        all_recommendation.append(recommendation)

    recommendation = pd.concat(all_recommendation)
    concatted = recommendation.sort_values("sim", ascending=False).head(30)

    concatted["artists_upd_v1"] = concatted["artists"].apply(
        lambda x: re.findall(r"'([^']*)'", x)
    )
    concatted["artists_upd_v2"] = concatted["artists"].apply(
        lambda x: re.findall('"(.*?)"', x)
    )
    concatted["artists_upd"] = np.where(
        concatted["artists_upd_v1"].apply(lambda x: not x),
        concatted["artists_upd_v2"],
        concatted["artists_upd_v1"],
    )
    concatted["artist"] = concatted["artists_upd"].apply(lambda x: x[0])

    access_token = get_new_access_token_from_refresh_token(refresh_token)
    concatted["url"] = concatted["id"].apply(
        lambda id: get_track_details(id=id, access_token=access_token)
    )
    # recommendations["artist"] = recommendations["artist"].apply(
    #     lambda x: x[0].replace("_", " ")
    # )
    concatted = concatted[["year", "artist", "id", "name", "popularity", "sim", "url"]]

    csv = concatted.to_csv(index=False).encode("utf-8")
    data_buffer_binary = io.BytesIO(csv)

    return minio_client.put_object(
        bucket_name="spotify-recommendation",
        object_name=f"recommendation_{file_name}",
        data=data_buffer_binary,
        length=len(csv),
        content_type="application/csv",
    )
