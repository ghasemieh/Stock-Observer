import smtplib
from email.message import EmailMessage
import configuration
from configparser import ConfigParser
from log_setup import get_logger

logger = get_logger(__name__)


class Notifier:
    def __init__(self, config: ConfigParser):
        self.config = config
        self.alireza_address = config['Email']['alireza address']
        self.mehrdad_address = config['Email']['mehrdad address']

    def notifier(self) -> None:
        logger.info("Notifier started")
        try:
            f = open("email_credential.txt", "r")

            sender = f.readline()
            password = f.readline()

            contacts = [self.alireza_address, self.mehrdad_address]

            msg = EmailMessage()
            msg['Subject'] = 'Test email from Stock Observer'
            msg['From'] = sender
            msg['To'] = ', '.join(contacts)
            msg.set_content(f'Hello\nYou received this email as a test to confirm the notifier functionality.' \
                            f'\nPlease ignore it\nThanks')

            with open('logs/pipeline.log', 'r') as f:
                file_data = f.read()
                file_name = f.name

            msg.add_attachment(file_data, filename=file_name)

            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(sender, password)
                smtp.send_message(msg)
                logger.info("Email Sent")

        except Exception as e:
            logger.error(e)


if __name__ == '__main__':
    notifier = Notifier(configuration.get())
    notifier.notifier()
