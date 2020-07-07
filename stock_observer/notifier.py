
import configuration
from configparser import ConfigParser
from log_setup import get_logger
import email, smtplib, ssl

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = get_logger(__name__)


class Notifier:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.alireza_address = config['Email']['alireza address']
        self.mehrdad_address = config['Email']['mehrdad address']
        self.equity_price = config['Data_Sources']['equity price csv']

    def notifier(self) -> None:
        logger.info("Notifier started")
        try:
            credential = open("email_credential.txt", "r")

            sender_email = credential.readline()
            password = credential.readline()

            receiver_email = [self.alireza_address, self.mehrdad_address]

            message = MIMEMultipart("alternative")
            message['Subject'] = 'Daily Equity Price from Stock Observer'
            message['From'] = sender_email
            message['To'] = ', '.join(receiver_email)

            # Create the plain-text and HTML version of your message
            text = """\
            Hi,
            Check the daily equity price in the attachment.
            """

            html = """\
            <html>
              <body>
                <p>Hi,<br>
                   Check the daily equity price in the attachment.<br>
                </p>
              </body>
            </html>
            """

            # Turn these into plain/html MIMEText objects
            part1 = MIMEText(text, "plain")
            part2 = MIMEText(html, "html")

            # Add HTML/plain-text parts to MIMEMultipart message
            # The email client will try to render the last part first
            message.attach(part1)
            message.attach(part2)

            filename = self.equity_price  # In same directory as script

            # Open PDF file in binary mode
            with open(filename, "rb") as attachment:
                # Add file as application/octet-stream
                # Email client can usually download this automatically as attachment
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())

            # Encode file in ASCII characters to send by email
            encoders.encode_base64(part)

            # Add header as key/value pair to attachment part
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {filename}",
            )

            # Add attachment to message and convert message to string
            message.attach(part)
            text = message.as_string()

            # msg.add_attachment(file_data, filename=file_name)
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
                smtp.login(sender_email, password)
                smtp.sendmail(sender_email, receiver_email, text)
                logger.info("Email Sent")

        except Exception as e:
            logger.error(e)


if __name__ == '__main__':
    notifier = Notifier(configuration.get())
    notifier.notifier()
