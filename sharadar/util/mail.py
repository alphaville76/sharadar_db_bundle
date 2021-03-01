import sys
from os import environ as env
from mailjet_rest import Client


def send_mail(subject, message):
    api_key = env["MJ_APIKEY_PUBLIC"]
    api_secret = env["MJ_APIKEY_PRIVATE"]
    mailjet = Client(auth=(api_key, api_secret), version='v3.1')
    data = {
        'Messages': [
            {
                "From": {
                    "Email": env["MJ_EMAIL"],
                    "Name": env["MJ_NAME"]
                },
                "To": [
                    {
                        "Email": env["MJ_EMAIL"],
                        "Name": env["MJ_NAME"]
                    }
                ],
                "Subject": subject,
                "TextPart": message
            }
        ]
    }
    result = mailjet.send.create(data=data)
    return result.status_code == 200

if __name__ == '__main__':
    subject = sys.argv[1]
    message = sys.argv[2]
    if not send_mail(subject, message):
        print("send failure!")
