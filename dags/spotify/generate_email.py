import os
from jinja2 import Environment, FileSystemLoader, select_autoescape
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

env = Environment(
    loader=FileSystemLoader(searchpath="dags/spotify/templates"),
    autoescape=select_autoescape(),
)

template = env.get_template("base.html")
GMAIL_EMAIL_PORT = os.getenv("GMAIL_EMAIL_PORT")
GMAIL_EMAIL_PASSWORD = os.getenv("GMAIL_EMAIL_PASSWORD")
GMAIL_EMAIL = os.getenv("GMAIL_EMAIL")
CONTEXT = ssl.create_default_context()


def create_email(df, username):
    df.rename(
        columns={"name": "title", "id": "spotify_link", "url": "img_url"}, inplace=True
    )
    ctx = {
        "name": username,
        "hero": df[0:1].to_dict("records"),
        "cards": df[1:17].to_dict("records"),
    }
    output = template.render(ctx=ctx)
    return output


def send_email(reciever_email: str, html_content: str):
    # Create a secure SSL context
    message = MIMEMultipart("alternative")
    message["Subject"] = "Here's your Noiist recommendations."
    message["From"] = GMAIL_EMAIL
    message["To"] = reciever_email

    html_part = MIMEText(html_content, "html")
    message.attach(html_part)

    with smtplib.SMTP_SSL(
        "smtp.gmail.com", GMAIL_EMAIL_PORT, context=CONTEXT
    ) as server:
        server.login(GMAIL_EMAIL, GMAIL_EMAIL_PASSWORD)
        server.sendmail(GMAIL_EMAIL, reciever_email, message.as_string())
