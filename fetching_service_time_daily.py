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
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType,Disposition
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
    # ADD THESE 4 LINES HERE:
    if 'kitchen_received' in df.columns:
        df['kitchen_received'] = df['kitchen_received'].dt.tz_localize(None)
    if 'kitchen_done' in df.columns:
        df['kitchen_done'] = df['kitchen_done'].dt.tz_localize(None)
    print(f"📊 Total orders collected: {len(df)}")
    
    # Filter out orders with missing period_minutes
    df_with_periods = df[df['period_minutes'].notna()].copy()
    
    print(f"📊 Orders with valid preparation times: {len(df_with_periods)}")
    
    if len(df_with_periods) == 0:
        print("❌ No orders with valid preparation times found")
        return None
    
    # Create the specific report with your required columns
    branch_report = df_with_periods.groupby(['branch_id', 'branch_name']).agg({
        'period_minutes': ['count', 'mean'],  # count for total orders, mean for average duration
    }).reset_index()
    
    # Flatten column names
    branch_report.columns = ['branch_code', 'branch_name', 'total_orders', 'average_duration_orders']
    
    # Calculate delayed orders (orders > 15 minutes)
    delayed_orders = df_with_periods[df_with_periods['period_minutes'] > 15].groupby(['branch_id', 'branch_name']).size().reset_index(name='delayed_orders')
    delayed_orders.columns = ['branch_code', 'branch_name', 'delayed_orders']
    
    # Merge the delayed orders data
    branch_report = branch_report.merge(
        delayed_orders[['branch_code', 'delayed_orders']], 
        on='branch_code', 
        how='left'
    )
    
    # Fill NaN values with 0 for branches with no delayed orders
    branch_report['delayed_orders'] = branch_report['delayed_orders'].fillna(0).astype(int)
    
    # Calculate percentage of delayed orders
    branch_report['% of delayed orders'] = (
        (branch_report['delayed_orders'] / branch_report['total_orders']) * 100
    ).round(2)
    
    # Round average duration to 2 decimal places
    branch_report['average_duration_orders'] = branch_report['average_duration_orders'].round(2)
    
    # Reorder columns to match your requirements
    branch_report = branch_report[[
        'branch_code', 
        'branch_name', 
        'total_orders', 
        'delayed_orders', 
        '% of delayed orders', 
        'average_duration_orders'
    ]]
    
    # Create Excel file with the specific report
    bus_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    filename = f'/tmp/kitchen_performance_report_{bus_date}.xlsx'
    
    print(f"📁 Saving Excel file to: {filename}")
    
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Main sheet with your specific columns
            branch_report.to_excel(writer, sheet_name='Kitchen Performance Report', index=False)
            
            # Optional: Add a detailed sheet with all orders (you can remove this if not needed)
            df_with_periods.to_excel(writer, sheet_name='Detailed Orders', index=False)
        
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
        
        # Check if file exists before trying to attach
        if not os.path.exists(filename):
            print(f"❌ File {filename} does not exist!")
            return
        
        # Get file size for debugging
        file_size = os.path.getsize(filename)
        print(f"📁 File size: {file_size} bytes")
        
        # Check if file is too large (SendGrid limit is 30MB)
        if file_size > 30 * 1024 * 1024:  # 30MB in bytes
            print(f"❌ File too large for email attachment: {file_size / 1024 / 1024:.2f}MB")
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
        try:
            with open(filename, 'rb') as f:
                data = f.read()
                encoded_file = base64.b64encode(data).decode()
            
            # Just the filename without path for the attachment name
            attachment_filename = os.path.basename(filename)
            
            attachment = Attachment(
                FileContent(encoded_file),
                FileName(attachment_filename),
                FileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                Disposition("attachment")
            )
            
            message.add_attachment(attachment)
            print(f"✅ Attachment added: {attachment_filename}")
            
        except Exception as attach_error:
            print(f"❌ Error creating attachment: {attach_error}")
            return
        
        # Send email
        sg = SendGridAPIClient(api_key=SENDGRID_API_KEY)
        response = sg.send(message)
        
        print(f"📧 Email sent! Status: {response.status_code}")
        
        if response.status_code == 202:
            print("✅ Email accepted by SendGrid")
        else:
            print(f"⚠️ Unexpected status code: {response.status_code}")
        
        # Clean up: Delete the temporary file after sending
        try:
            os.remove(filename)
            print(f"🗑️ Temporary file deleted: {filename}")
        except Exception as cleanup_error:
            print(f"⚠️ Could not delete temporary file: {cleanup_error}")
        
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
