import requests
import json
from datetime import datetime,timedelta
import time 
from zoneinfo import ZoneInfo
import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import psycopg2


# Load environment variables
load_dotenv()

# Global list to store all orders
all_orders = []

def convert_api_datetime_to_local(date_string):
    """Convert API datetime string from UTC to local time (UTC+3)"""
    if not date_string:
        return None
    utc_time = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("UTC"))
    local_time = utc_time.astimezone(ZoneInfo("Asia/Riyadh"))
    return local_time

def operating(TOKEN, BASE_URL, order_ref=0):
    global all_orders
    all_orders = []  # Reset the list
    
    # Define the endpoint and parameters
    endpoint = "/orders"
    page = 1
    has_more_pages = True
    bus_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Rate limiting configuration
    max_retries = 3
    base_delay = 2  # Base delay between requests (increased from 1 to 2 seconds)
    
    print(f"📅 Fetching orders for business date: {bus_date}")
    
    while has_more_pages:
        params = {
            "page": page,
            "filter[business_date]": bus_date,
            "filter[status]": "4",
            "include": "branch",
            "sort": "-created_at"
        }
        
        # Set headers with token
        headers = {
            "Authorization": f"Bearer {TOKEN}"
        }

        # Retry logic for rate limiting
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            # Make the request
            response = requests.get(BASE_URL + endpoint, headers=headers, params=params)

            # Check response
            if response.status_code == 200:
                data = response.json()
                extracting(data['data'])

                print(f"✅ Success! Page {page} data received")
                
                meta = data['meta']
                current_page = meta['current_page']
                last_page = meta['last_page']
                
                print(f"📊 Progress: {current_page}/{last_page} pages | Total orders collected: {len(all_orders)}")
                
                if current_page >= last_page:
                    has_more_pages = False
                else:
                    page += 1
                    time.sleep(base_delay)  # Wait between successful requests
                
                success = True
                
            elif response.status_code == 429:
                retry_count += 1
                # Exponential backoff: wait longer each retry
                wait_time = base_delay * (2 ** retry_count)  # 4s, 8s, 16s
                print(f"⚠️ Rate limit hit (429) on page {page}. Retry {retry_count}/{max_retries}. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                
                if retry_count >= max_retries:
                    print(f"❌ Max retries reached for page {page}. Continuing to next page...")
                    page += 1  # Skip this page and continue
                    success = True  # Exit retry loop
                    
            elif response.status_code == 504:
                print("❌ Timeout error (504) — try a smaller date range or check the server.")
                has_more_pages = False
                break
            else:
                print(f"❌ Error {response.status_code}: {response.text}")
                has_more_pages = False
                break
    
    # After collecting all data, create DataFrame and Excel
    if all_orders:
        print(f"\n📊 Final count: {len(all_orders)} orders collected")
        create_excel_report()
    else:
        print("❌ No orders data collected")

def extracting(data):
    global all_orders
    
    for i in data:
        try:
            branch_id = i['branch']['reference']
            branch_name = i['branch']['name_localized']
            order_ref = i['reference']
            exc_vat_price = i['subtotal_price']
            business_date = i['business_date']
            
            # Fixed: access kitchen times from individual order, not data['meta']
            kitchen_rec_str = i.get('meta', {}).get('foodics', {}).get('kitchen_received_at')
            kitchen_done_str = i.get('meta', {}).get('foodics', {}).get('kitchen_done_at')
            
            # Convert to local time
            kitchen_rec = convert_api_datetime_to_local(kitchen_rec_str) if kitchen_rec_str else None
            kitchen_done = convert_api_datetime_to_local(kitchen_done_str) if kitchen_done_str else None
            
            # Calculate period in minutes
            period_minutes = None
            if kitchen_rec and kitchen_done:
                period_minutes = round((kitchen_done - kitchen_rec).total_seconds() / 60, 2)

            # Append to global list
            all_orders.append({
                'order_ref': order_ref,
                'branch_id': branch_id,
                'branch_name': branch_name,
                'exc_vat_price': exc_vat_price,
                'business_date': business_date,
                'kitchen_received': kitchen_rec,
                'kitchen_done': kitchen_done,
                'period_minutes': period_minutes
            })
            
        except KeyError as e:
            print(f"❌ Missing key in order data: {e}")
            continue
        except Exception as e:
            print(f"❌ Error processing order: {e}")
            continue
    
    print(f"✅ Processed {len(data)} orders from this page")

def create_excel_report():
    global all_orders
    
    # Create DataFrame from all collected orders
    df = pd.DataFrame(all_orders)
    # Remove timezone info for Excel compatibility
    if 'kitchen_received' in df.columns:
        df['kitchen_received'] = df['kitchen_received'].dt.tz_localize(None)
    if 'kitchen_done' in df.columns:
        df['kitchen_done'] = df['kitchen_done'].dt.tz_localize(None)
    
    print(f"📊 Total orders collected: {len(df)}")
    
    # Separate orders with valid and invalid preparation times
    df_with_periods = df[df['period_minutes'].notna()].copy()
    df_without_periods = df[df['period_minutes'].isna()].copy()
    
    print(f"📊 Orders with valid preparation times: {len(df_with_periods)}")
    print(f"⚠️ Orders with invalid/missing preparation times: {len(df_without_periods)}")
    
    # Create report with ALL orders (valid + invalid)
    # First, get counts for ALL orders per branch
    all_orders_count = df.groupby(['branch_id', 'branch_name']).size().reset_index(name='total_orders')
    all_orders_count.columns = ['branch_code', 'branch_name', 'total_orders']
    
    # Get counts for orders with INVALID preparation times
    if len(df_without_periods) > 0:
        invalid_orders_count = df_without_periods.groupby(['branch_id', 'branch_name']).size().reset_index(name='invalid_orders')
        invalid_orders_count.columns = ['branch_code', 'branch_name', 'invalid_orders']
    else:
        invalid_orders_count = pd.DataFrame(columns=['branch_code', 'branch_name', 'invalid_orders'])
    
    # Calculate metrics for orders with VALID preparation times
    if len(df_with_periods) > 0:
        valid_metrics = df_with_periods.groupby(['branch_id', 'branch_name']).agg({
            'period_minutes': 'mean'  # average duration
        }).reset_index()
        valid_metrics.columns = ['branch_code', 'branch_name', 'average_duration_orders']
        
        # Calculate delayed orders (orders > 15 minutes) from valid orders only
        delayed_orders = df_with_periods[df_with_periods['period_minutes'] > 15].groupby(['branch_id', 'branch_name']).size().reset_index(name='delayed_orders')
        delayed_orders.columns = ['branch_code', 'branch_name', 'delayed_orders']
    else:
        print("❌ No orders with valid preparation times found")
        return None
    
    # Merge all data together
    branch_report = all_orders_count.copy()
    
    # Merge invalid orders count
    branch_report = branch_report.merge(
        invalid_orders_count[['branch_code', 'invalid_orders']], 
        on='branch_code', 
        how='left'
    )
    branch_report['invalid_orders'] = branch_report['invalid_orders'].fillna(0).astype(int)
    
    # Merge delayed orders
    branch_report = branch_report.merge(
        delayed_orders[['branch_code', 'delayed_orders']], 
        on='branch_code', 
        how='left'
    )
    branch_report['delayed_orders'] = branch_report['delayed_orders'].fillna(0).astype(int)
    
    # Merge average duration
    branch_report = branch_report.merge(
        valid_metrics[['branch_code', 'average_duration_orders']], 
        on='branch_code', 
        how='left'
    )
    
    # Calculate percentage of delayed orders (based on valid orders only)
    # valid_orders = total_orders - invalid_orders
    branch_report['valid_orders'] = branch_report['total_orders'] - branch_report['invalid_orders']
    branch_report['% of delayed orders'] = (
        (branch_report['delayed_orders'] / branch_report['valid_orders']) * 100
    ).round(2)
    
    # Handle division by zero (if valid_orders is 0)
    branch_report['% of delayed orders'] = branch_report['% of delayed orders'].fillna(0)
    
    # Round average duration to 2 decimal places
    branch_report['average_duration_orders'] = branch_report['average_duration_orders'].round(2)
    
    # Reorder columns to match your requirements
    branch_report = branch_report[[
        'branch_code', 
        'branch_name', 
        'total_orders',          # ALL orders (valid + invalid)
        'invalid_orders',        # NEW: Orders without valid preparation times
        'valid_orders',          # NEW: Orders with valid preparation times
        'delayed_orders',        # Orders > 15 minutes (from valid orders)
        '% of delayed orders',   # Percentage based on valid orders
        'average_duration_orders' # Average duration (from valid orders)
    ]]
    
    # Create Excel file with the specific report
    bus_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    filename = f'/tmp/kitchen_performance_report_{bus_date}.xlsx'
    
    print(f"📁 Saving Excel file to: {filename}")
    
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Main sheet with your specific columns
            branch_report.to_excel(writer, sheet_name='Kitchen Performance Report', index=False)
            
            # Optional: Add detailed sheets
            if len(df_with_periods) > 0:
                df_with_periods.to_excel(writer, sheet_name='Valid Orders', index=False)
            if len(df_without_periods) > 0:
                df_without_periods.to_excel(writer, sheet_name='Invalid Orders', index=False)
        
        # Verify file was created successfully
        if os.path.exists(filename):
            file_size = os.path.getsize(filename)
            print(f"✅ Excel file created successfully: {filename} ({file_size} bytes)")
        else:
            print(f"❌ Failed to create Excel file: {filename}")
            return None
            
    except Exception as e:
        print(f"❌ Error creating Excel file: {e}")
        return None
    
    print(f"📊 Excel report created: {filename}")
    print("\n📈 Kitchen Performance Report:")
    print(branch_report.to_string(index=False))
    
    # Send email with the report
    send_email_report(filename)
    
    return filename

def send_email_report(filename):
    """Send the Excel report via SMTP (Gmail)"""
    try:
        # Email configuration from environment variables
        SENDER_EMAIL = os.environ.get('SENDER_EMAIL')
        SENDER_PASSWORD = os.environ.get('SENDER_PASSWORD')  # Gmail App password
        RECIPIENT_EMAILS = os.environ.get('RECIPIENT_EMAIL')
        
        if not all([SENDER_EMAIL, SENDER_PASSWORD, RECIPIENT_EMAILS]):
            print("❌ Missing email configuration in environment variables")
            print("Required: SENDER_EMAIL, SENDER_PASSWORD, RECIPIENT_EMAIL")
            return
        
        email_list = [email.strip() for email in RECIPIENT_EMAILS.split(',')]
        
        # Check if file exists before trying to attach
        if not os.path.exists(filename):
            print(f"❌ File {filename} does not exist!")
            return
        
        # Get file size for debugging
        file_size = os.path.getsize(filename)
        print(f"📁 File size: {file_size} bytes")
        
        # Check if file is too large (Gmail limit is 25MB)
        if file_size > 25 * 1024 * 1024:  # 25MB in bytes
            print(f"❌ File too large for email attachment: {file_size / 1024 / 1024:.2f}MB")
            return
        
        bus_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
   
        # Create message
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = ', '.join(email_list)
        msg['Subject'] = f'{bus_date} - تقرير زمن الخدمة'
        
        # Email body
        body = f'''
        <h2>تقرير زمن الخدمة</h2>
        <p><strong>{bus_date}</strong> مرفق لكم تقرير زمن الخدمة ليوم </p>
        <p>الملف يحتوي على:</p>
        <ul>
            <li>عدد الطلبات الكلي</li>
            <li>الطلبات التى تجاوزت ال 15 دقيقة (الطلبات المتأخرة)</li>
            <li>نسبة الطلبات المتأخرة</li>
            <li>متوسط زمن الخدمة</li>
        </ul>
        <p>Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        '''
        
        msg.attach(MIMEText(body, 'html'))
        
        # Attach Excel file
        try:
            with open(filename, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            
            encoders.encode_base64(part)
            
            attachment_filename = os.path.basename(filename)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {attachment_filename}'
            )
            
            msg.attach(part)
            print(f"✅ Attachment added: {attachment_filename}")
            
        except Exception as attach_error:
            print(f"❌ Error creating attachment: {attach_error}")
            return
        
        # Send email via Gmail SMTP
        print(f"📧 Connecting to Gmail SMTP...")
        
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()  # Enable encryption
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        
        text = msg.as_string()
        server.sendmail(SENDER_EMAIL, email_list, text)
        server.quit()
        
        print("✅ Email sent successfully!")
        
        # Clean up: Delete the temporary file after sending
        try:
            os.remove(filename)
            print(f"🗑️ Temporary file deleted: {filename}")
        except Exception as cleanup_error:
            print(f"⚠️ Could not delete temporary file: {cleanup_error}")
        
    except smtplib.SMTPAuthenticationError:
        print("❌ SMTP Authentication failed!")
        print("Make sure you're using a Gmail App Password, not your regular password")
    except smtplib.SMTPException as smtp_error:
        print(f"❌ SMTP Error: {smtp_error}")
    except Exception as e:
        print(f"❌ Error sending email: {e}")
        import traceback
        traceback.print_exc()

# Main execution
if __name__ == "__main__":
    TOKEN = os.environ.get('API_TOKEN')
    BASE_URL = os.environ.get('BASE_URL')
    
    if not TOKEN or not BASE_URL:
        print("❌ Missing API_TOKEN or BASE_URL in environment variables")
    else:
        operating(TOKEN, BASE_URL)

