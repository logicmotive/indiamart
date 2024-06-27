from __future__ import unicode_literals
import frappe
from frappe import throw, msgprint, _
from datetime import date
import json
import requests
import time

@frappe.whitelist()
def add_source_lead():
    if not frappe.db.exists("Lead Source", "India Mart"):
        doc = frappe.get_doc({
            "doctype": "Lead Source",
            "source_name": "India Mart"
        }).insert(ignore_permissions=True)
        if doc:
            frappe.msgprint(_("Lead Source Added For India Mart"))
    else:
        frappe.msgprint(_("India Mart Lead Source Already Available"))

@frappe.whitelist()
def sync_india_mart_lead(from_date, to_date):
    try:
        india_mart_setting = frappe.get_doc("IndiaMart Setting", "IndiaMart Setting")
        if not (india_mart_setting.url and india_mart_setting.mobile_no and india_mart_setting.key):
            frappe.throw(
                msg=_('URL, Mobile, Key mandatory for Indiamart API Call. Please set them and try again.'),
                title=_('Missing Setting Fields')
            )
        req = get_request_url(india_mart_setting, from_date, to_date)
        
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            res = requests.post(url=req)
            
            if res.status_code == 429:
                if attempt < max_retries - 1:
                    frappe.msgprint(_("Rate limit exceeded. Retrying in {} seconds...").format(retry_delay))
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    frappe.msgprint(_("Rate limit exceeded. Please try again later."))
                    return
            elif res.status_code != 200:
                frappe.msgprint(_("API request failed with status code: {}").format(res.status_code))
                return
            else:
                break  # If we didn't get a 429 or other error, break out of the retry loop
        
        if res.text:
            try:
                response_data = json.loads(res.text)
            except json.JSONDecodeError:
                frappe.msgprint(_("Failed to parse API response as JSON. Raw response: {}").format(res.text[:1000]))
                return

            frappe.msgprint(_("API Response: {}").format(json.dumps(response_data, indent=2)))

            if isinstance(response_data, dict):
                if response_data.get("CODE") == 429:
                    frappe.msgprint(_("Rate limit exceeded. Please try again later."))
                    return
                elif "Error_Message" in response_data:
                    frappe.throw(response_data["Error_Message"])
                else:
                    frappe.msgprint(_("Response is a dictionary, expected a list. Response: {}").format(json.dumps(response_data, indent=2)))
                    return
            elif not isinstance(response_data, list):
                frappe.msgprint(_("Unexpected response type. Expected list, got {}. Response: {}").format(type(response_data), json.dumps(response_data, indent=2)))
                return
            
            frappe.msgprint(_("Total records received: {}").format(len(response_data)))
            
            leads_created = 0
            leads_existing = 0
            leads_failed = 0
            
            for row in response_data:
                if isinstance(row, dict):
                    result = add_lead(row)
                    if result == True:
                        leads_created += 1
                    elif result == "EXISTS":
                        leads_existing += 1
                    else:
                        leads_failed += 1
                        frappe.msgprint(_("Failed to add lead: {}").format(row.get('UNIQUE_QUERY_ID', 'Unknown ID')))
                else:
                    frappe.msgprint(_("Unexpected row type. Expected dict, got {}. Row: {}").format(type(row), row))
            
            frappe.msgprint(_("Sync Results:\nCreated: {}\nAlready Existing: {}\nFailed: {}").format(
                leads_created, leads_existing, leads_failed))
            
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("India Mart Sync Error"))
        frappe.throw(_("An error occurred while syncing leads: {}").format(str(e)))

def get_request_url(india_mart_setting, from_date, to_date):
    return (f"{india_mart_setting.url}?"
            f"glusr_crm_key={india_mart_setting.key}&"
            f"start_time={from_date}&"
            f"end_time={to_date}")

@frappe.whitelist()
def cron_sync_lead():
    try:
        today = frappe.utils.today()
        sync_india_mart_lead(today, today)
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("India Mart Sync Error"))
        frappe.throw(_("An error occurred during cron sync. Please check the error log."))

# def add_lead(lead_data):
#     try:
#         if not frappe.db.exists("Lead", {"custom_indiamart_id": lead_data["UNIQUE_QUERY_ID"]}):
#             doc = frappe.get_doc({
#                 "doctype": "Lead",
#                 "first_name": lead_data["SENDER_NAME"],
#                 "mobile_no": lead_data["SENDER_MOBILE"],
#                 "company_name": lead_data.get("SENDER_COMPANY", ""),
#                 "source": "India Mart",
#                 "custom_indiamart_id": lead_data["UNIQUE_QUERY_ID"]
#             })
#             doc.insert(ignore_permissions=True)
#             frappe.db.commit()  # Commit the transaction
#             return True
#         return "EXISTS"
#     except Exception as e:
#         frappe.log_error(frappe.get_traceback(), _("Error adding lead from India Mart: {}").format(str(e)))
#         return False
def add_lead(lead_data):
    try:
        if not frappe.db.exists("Lead", {"custom_indiamart_id": lead_data["UNIQUE_QUERY_ID"]}):
            frappe.logger().info(f"Attempting to create lead with ID: {lead_data['UNIQUE_QUERY_ID']}")
            doc = frappe.get_doc({
                "doctype": "Lead",
                "first_name": lead_data["SENDER_NAME"],
                "mobile_no": lead_data["SENDER_MOBILE"],
                "company_name": lead_data.get("SENDER_COMPANY", ""),
                "status": "Lead",
                "source": "India Mart",
                "custom_indiamart_id": lead_data["UNIQUE_QUERY_ID"]
            })
            doc.insert(ignore_permissions=True)
            frappe.db.commit()
            frappe.logger().info(f"Successfully created lead with ID: {lead_data['UNIQUE_QUERY_ID']}")
            return True
        else:
            frappe.logger().info(f"Lead already exists with ID: {lead_data['UNIQUE_QUERY_ID']}")
        return "EXISTS"
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("Error adding lead from India Mart: {}").format(str(e)))
        frappe.logger().error(f"Failed to create lead with ID: {lead_data.get('UNIQUE_QUERY_ID', 'Unknown')}. Error: {str(e)}")
        return False
