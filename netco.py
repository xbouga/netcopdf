import smtplib
import dns.resolver
from email.message import EmailMessage
from email.utils import formataddr
import email.utils
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# CONFIGURATION
BASE_SENDER_EMAIL = "sarah-schneider@smce.eu"
SENDER_NAME = "Techniker Kundenservice"
SUBJECT = "Nur kurz zur Erinnerung {rand}"  # Use {rand} placeholder here
ATTACH_PDF = True
PDF_FILE = "PDF_Informationen.pdf"
USE_PLAIN_TEXT = True  # Send only plain text message from message.txt

MAX_RETRIES = 3
RETRY_DELAY = 5

SUCCESS_LOG = "success_log.txt"
FAIL_LOG = "fail_log.txt"

SEND_IN_BCC = True  # Set True to send batch emails in BCC, False to send individual emails

# Generate random sender email
local_part, domain = BASE_SENDER_EMAIL.split('@')
random_number = random.randint(1000, 9999)
SENDER_EMAIL = f"{local_part.lower()}.{random_number}@{domain}"
print(f"[i] Using random sender email: {SENDER_EMAIL}")

# Load recipients
with open("mail.txt", "r") as f:
    recipients = [line.strip() for line in f if line.strip()]

# Load plain text message
plain_text = "This is a fallback plain text message."
if USE_PLAIN_TEXT and os.path.exists("message.txt"):
    with open("message.txt", "r", encoding="utf-8") as f:
        plain_text = f.read()

def log_message(filename, message):
    with open(filename, "a", encoding="utf-8") as f:
        f.write(message + "\n")

def send_message(msg, mx_record):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with smtplib.SMTP(mx_record, 25, timeout=10) as server:
                server.ehlo()
                server.send_message(msg)
            return True
        except Exception as e:
            print(f"[-] Attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return False

def attach_pdf_randomized(msg):
    if ATTACH_PDF and os.path.exists(PDF_FILE):
        print(f"[i] Attaching PDF file: {PDF_FILE}")
        with open(PDF_FILE, 'rb') as f:
            pdf_data = f.read()
        rand_pdf_num = random.randint(1000, 9999)
        random_pdf_name = f"PDF_Informationen_{rand_pdf_num}.pdf"
        msg.add_attachment(pdf_data, maintype='application', subtype='pdf', filename=random_pdf_name)
    else:
        print("[!] PDF file not found or attachment disabled")

def get_mx_record(domain):
    answers = dns.resolver.resolve(domain, 'MX')
    mx_record = sorted([(r.preference, r.exchange.to_text()) for r in answers])[0][1]
    return mx_record

def process_recipient(recipient):
    # Single recipient processing (individual send)
    try:
        domain = recipient.split('@')[1]
        mx_record = get_mx_record(domain)

        msg = EmailMessage()
        msg['From'] = formataddr((SENDER_NAME, SENDER_EMAIL))
        msg['To'] = recipient

        rand_num = random.randint(1000, 9999)
        subject_to_send = SUBJECT.replace("{rand}", str(rand_num))
        plain_text_to_send = plain_text.replace("{rand}", str(rand_num))

        msg['Subject'] = subject_to_send
        unique_id = f"{int(time.time())}.{random.randint(1000,9999)}@{domain}"
        msg['Message-ID'] = f"<{unique_id}>"
        msg['Date'] = email.utils.formatdate(localtime=True)
        msg['Reply-To'] = SENDER_EMAIL
        unsubscribe_email = SENDER_EMAIL
        msg['List-Unsubscribe'] = f"<mailto:{unsubscribe_email}?subject=unsubscribe>"

        msg.set_content(plain_text_to_send)
        attach_pdf_randomized(msg)

        print(f"[i] Sending to: {recipient} with subject: {subject_to_send}")
        success = send_message(msg, mx_record)
        if success:
            print(f"[+] Sent to: {recipient}")
            log_message(SUCCESS_LOG, recipient)
            return (recipient, True)
        else:
            print(f"[-] Failed to send after retries: {recipient}")
            log_message(FAIL_LOG, recipient)
            return (recipient, False)

    except Exception as e:
        print(f"[-] Failed to send to {recipient}: {e}")
        log_message(FAIL_LOG, recipient + " | Exception: " + str(e))
        return (recipient, False)

def process_recipients_bcc(recipients_batch):
    # Send a single email where first recipient is To, others in Bcc
    try:
        to_address = recipients_batch[0]
        bcc_recipients = recipients_batch[1:] if len(recipients_batch) > 1 else []

        domain = to_address.split('@')[1]
        mx_record = get_mx_record(domain)

        msg = EmailMessage()
        msg['From'] = formataddr((SENDER_NAME, SENDER_EMAIL))
        msg['To'] = to_address
        if bcc_recipients:
            msg['Bcc'] = ", ".join(bcc_recipients)

        rand_num = random.randint(1000, 9999)
        subject_to_send = SUBJECT.replace("{rand}", str(rand_num))
        plain_text_to_send = plain_text.replace("{rand}", str(rand_num))

        msg['Subject'] = subject_to_send
        unique_id = f"{int(time.time())}.{random.randint(1000,9999)}@{domain}"
        msg['Message-ID'] = f"<{unique_id}>"
        msg['Date'] = email.utils.formatdate(localtime=True)
        msg['Reply-To'] = SENDER_EMAIL
        unsubscribe_email = SENDER_EMAIL
        msg['List-Unsubscribe'] = f"<mailto:{unsubscribe_email}?subject=unsubscribe>"

        msg.set_content(plain_text_to_send)
        attach_pdf_randomized(msg)

        print(f"[i] Sending batch BCC email: To={to_address}, Bcc count={len(bcc_recipients)}")
        success = send_message(msg, mx_record)
        if success:
            for r in recipients_batch:
                log_message(SUCCESS_LOG, r)
            print(f"[+] Batch BCC email sent successfully")
            return [(r, True) for r in recipients_batch]
        else:
            for r in recipients_batch:
                log_message(FAIL_LOG, r)
            print(f"[-] Failed to send batch BCC email")
            return [(r, False) for r in recipients_batch]

    except Exception as e:
        for r in recipients_batch:
            log_message(FAIL_LOG, r + " | Exception: " + str(e))
        print(f"[-] Exception sending batch BCC email: {e}")
        return [(r, False) for r in recipients_batch]

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

MAX_THREADS = 1
BATCH_SIZE = 10
DELAY_BETWEEN_BATCHES = 10  # seconds

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    for batch_num, batch_recipients in enumerate(chunks(recipients, BATCH_SIZE), 1):
        if SEND_IN_BCC:
            # Send one email per batch with first recipient in To, rest in BCC
            results = process_recipients_bcc(batch_recipients)
            # No multithreading needed here because it's one batch email
        else:
            # Send individual emails multithreaded
            futures = {executor.submit(process_recipient, recipient): recipient for recipient in batch_recipients}
            for future in as_completed(futures):
                recipient = futures[future]
                try:
                    result = future.result()
                    # Optionally handle result here
                except Exception as exc:
                    print(f"[-] Exception occurred for {recipient}: {exc}")

        print(f"[i] Batch {batch_num} done. Waiting {DELAY_BETWEEN_BATCHES} seconds before next batch...")
        time.sleep(DELAY_BETWEEN_BATCHES)
