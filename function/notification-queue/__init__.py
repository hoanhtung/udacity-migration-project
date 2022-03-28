import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
# from sendgrid import SendGridAPIClient
# from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s', notification_id)

    # TODO: Get connection to database
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PWD')
    )
    
    cur = conn.cursor()
    try:
        # TODO: Get notification message and subject from database using the notification_id
        # cur.execute('select * from notification where id=%s', (notification_id, ))
        # notification = cur.fetchone()

        # TODO: Get attendees email and name
        cur.execute('select * from attendee')
        attendees = cur.fetchall()
        # TODO: Loop through each attendee and send an email with a personalized subject

        # I uncomment because no using send_email func
        # for attendee in attendees:
            # subject = '{}: {}'.format(attendee.first_name, notification.subject)
            # send_email(attendee.email, subject, notification.message)

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        status = 'Notified {} attendees'.format(len(attendees))
        cur.execute("update notification set completed_date=%s, status=%s where id=%s", (datetime.utcnow(), status, notification_id, ))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        cur.close()



