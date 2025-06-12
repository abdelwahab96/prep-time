import requests
import json
from datetime import datetime,timedelta
import time 
from zoneinfo import ZoneInfo
# from psycopg2.extras import execute_values
import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType
import base64



# def check_ref_order():
#     #check last order in the postgres
#     # based on the buisness date if there is no data with the buisness data 

#     # check if there is data with this bdate if not, then:
#     # put the reference order as 0
#     # if yes check the last order id that appended based on the date
#     # take the id 
#     return

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
    
    while has_more_pages:
        params = {
            "page": page,
            "filter[business_date]": bus_date,
            "filter[status]": "4",
            "include": "branch",
            "sort": "-created_at",
            "filter[reference_after]": order_ref
        }
        
        # Set headers with token
        headers = {
            "Authorization": f"Bearer {TOKEN}"
        }

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
            
            if current_page >= last_page:
                has_more_pages = False
            else:
                page += 1
                time.sleep(1)
            
        elif response.status_code == 504:
            print("❌ Timeout error (504) — try a smaller date range or check the server.")
            break
        else:
            print(f"❌ Error {response.status_code}: {response.text}")
            break
    
    # After collecting all data, create DataFrame and Excel
    if all_orders:
        create_excel_report()
    else:
        print("No orders data collected")

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
    
    print(f"📊 Total orders collected: {len(df)}")
    
    # Filter out orders with missing period_minutes
    df_with_periods = df[df['period_minutes'].notna()].copy()
    
    print(f"📊 Orders with valid preparation times: {len(df_with_periods)}")
    
    if len(df_with_periods) == 0:
        print("❌ No orders with valid preparation times found")
        return None
    
    # Group by branch and calculate average preparation time
    branch_summary = df_with_periods.groupby(['branch_id', 'branch_name']).agg({
        'period_minutes': ['mean', 'count', 'min', 'max'],
        'exc_vat_price': 'sum'
    }).round(2)
    
    # Flatten column names
    branch_summary.columns = ['avg_prep_time_minutes', 'order_count', 'min_prep_time', 'max_prep_time', 'total_revenue']
    branch_summary = branch_summary.reset_index()
    
    # Create Excel file with multiple sheets
    bus_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    filename = f'kitchen_performance_report_{bus_date}.xlsx'
    
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # Sheet 1: Branch Summary
        branch_summary.to_excel(writer, sheet_name='Branch Summary', index=False)
        
        # Sheet 2: All Orders Detail
        df.to_excel(writer, sheet_name='All Orders', index=False)
        
        # Sheet 3: Only orders with preparation times
        df_with_periods.to_excel(writer, sheet_name='Orders with Prep Times', index=False)
    
    print(f"📊 Excel report created: {filename}")
    print("\n📈 Branch Performance Summary:")
    print(branch_summary.to_string(index=False))
    
    # Send email with the report
    send_email_report(filename)
    
    return filename

def send_email_report(filename):
    """Send the Excel report via SendGrid"""
    try:
        # Email configuration from environment variables
        SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')
        SENDER_EMAIL = os.environ.get('SENDER_EMAIL')
        RECIPIENT_EMAILS = os.environ.get('RECIPIENT_EMAIL')
        email_list = [email.strip() for email in RECIPIENT_EMAILS.split(',')]
        if not all([SENDGRID_API_KEY, SENDER_EMAIL, RECIPIENT_EMAILS]):
            print("❌ Missing email configuration in environment variables")
            return
        
        bus_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # Create email
        message = Mail(
            from_email=SENDER_EMAIL,
            to_emails=email_list,
            subject=f'Daily Kitchen Performance Report - {bus_date}',
            html_content=f'''
            <h2>Daily Kitchen Performance Report</h2>
            <p>Please find attached the kitchen performance report for <strong>{bus_date}</strong>.</p>
            <p>The report includes:</p>
            <ul>
                <li>Branch performance summary with average preparation times</li>
                <li>All orders details</li>
                <li>Orders with preparation time analysis</li>
            </ul>
            <p>Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
            '''
        )
        
        # Attach Excel file
        with open(filename, 'rb') as f:
            data = f.read()
            encoded_file = base64.b64encode(data).decode()
        
        attachment = Attachment(
            FileContent(encoded_file),
            FileName(filename),
            FileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        )
        message.attachment = attachment
        
        # Send email
        sg = SendGridAPIClient(api_key=SENDGRID_API_KEY)
        response = sg.send(message)
        
        print(f"📧 Email sent successfully! Status: {response.status_code}")
        
    except Exception as e:
        print(f"❌ Error sending email: {e}")

# Main execution
if __name__ == "__main__":
    TOKEN = os.environ.get('API_TOKEN')
    BASE_URL = os.environ.get('BASE_URL')
    
    if not TOKEN or not BASE_URL:
        print("❌ Missing API_TOKEN or BASE_URL in environment variables")
    else:
        operating(TOKEN, BASE_URL)
