import ssl
import smtplib
import configuration
from email import encoders
from log_setup import get_logger
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from configparser import ConfigParser
from email.mime.multipart import MIMEMultipart

logger = get_logger(__name__)


class Notifier:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.alireza_address = config['Email']['alireza address']
        self.mehrdad_address = config['Email']['mehrdad address']
        self.equity_price = config['Data_Sources']['equity price csv']
        self.processed_equity_price = config['Data_Sources']['processed equity price csv']

    def notifier(self, message: str) -> None:
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
            text = f"""\
            Hi,
            Check the daily equity price in the attachment.
            {message}
            """

            html = f"""\
            <html>
              <body>
                <p>Hi,<br>
                   Check the daily equity price in the attachment.<br>
                   {message}<br>
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

            # Attachment Section --------------------------------
            filename1 = "logs/pipeline.log"  # In same directory as script
            # Open PDF file in binary mode
            with open(filename1, "rb") as attachment:
                # Add file as application/octet-stream
                # Email client can usually download this automatically as attachment
                attach1 = MIMEBase("application", "octet-stream")
                attach1.set_payload(attachment.read())

            # Encode file in ASCII characters to send by email
            encoders.encode_base64(attach1)

            # Add header as key/value pair to attachment part
            attach1.add_header(
                "Content-Disposition",
                f"attachment; filename= {filename1}",
            )

            # Add attachment to message and convert message to string
            message.attach(attach1)


            # Attachment 2 -------------------------
            # filename2 = self.processed_equity_price  # In same directory as script
            # with open(filename2, "rb") as attachment:
            #     # Add file as application/octet-stream
            #     # Email client can usually download this automatically as attachment
            #     attach2 = MIMEBase("application", "octet-stream")
            #     attach2.set_payload(attachment.read())
            #
            # encoders.encode_base64(attach2)
            #
            # attach2.add_header(
            #     "Content-Disposition",
            #     f"attachment; filename= {filename2}",
            # )
            #
            # message.attach(attach2)
            # ---------------------------------------

            text = message.as_string()

            context = ssl.create_default_context()
            with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
                smtp.login(sender_email, password)
                smtp.sendmail(sender_email, receiver_email, text)
                logger.info("Email Sent")

        except Exception as e:
            logger.error(e)
            raise e


if __name__ == '__main__':
    notifier = Notifier(configuration.get())
    notifier.notifier()
