from __future__ import unicode_literals
import frappe
from frappe import throw, _
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
            return "Lead Source Added For India Mart"
    else:
        return "India Mart Lead Source Already Available"

@frappe.whitelist()
def sync_india_mart_lead(from_date, to_date):
    try:
        india_mart_setting = frappe.get_doc("IndiaMart Setting", "IndiaMart Setting")
        if not (india_mart_setting.url and india_mart_setting.mobile_no and india_mart_setting.key):
            throw(_('URL, Mobile, Key mandatory for Indiamart API Call. Please set them and try again.'))
        
        req = get_request_url(india_mart_setting, from_date, to_date)
        
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            res = requests.post(url=req)
            
            if res.status_code == 429:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    return "Rate limit exceeded. Please try again later."
            else:
                break  # If we didn't get a 429, break out of the retry loop
        
        if res.text:
            response_data = json.loads(res.text)
            
            if isinstance(response_data, dict) and response_data.get("CODE") == 429:
                return "Rate limit exceeded. Please try again later."
            
            leads_created = 0
            for row in response_data:
                if isinstance(row, dict):
                    if "Error_Message" in row:
                        throw(row["Error_Message"])
                    else:
                        if add_lead(row):
                            leads_created += 1
            
            if leads_created > 0:
                return f"You have successfully added {leads_created} Lead(s)"
            else:
                return "No new leads were added"
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("India Mart Sync Error"))
        throw(_("An error occurred while syncing leads. Please check the error log."))

def get_request_url(india_mart_setting, from_date, to_date):
    return (f"{india_mart_setting.url}?"
            f"glusr_crm_key={india_mart_setting.key}&"
            f"start_time={from_date}&"
            f"end_time={to_date}")

@frappe.whitelist()
def cron_sync_lead():
    try:
        today = frappe.utils.today()
        return sync_india_mart_lead(today, today)
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("India Mart Sync Error"))
        throw(_("An error occurred during cron sync. Please check the error log."))

def add_lead(lead_data):
    try:
        if not frappe.db.exists("Lead", {"custom_indiamart_id": lead_data["UNIQUE_QUERY_ID"]}):
            doc = frappe.get_doc({
                "doctype": "Lead",
                "first_name": lead_data["SENDER_NAME"],
                "email_id": lead_data.get("SENDER_EMAIL", ""),
                "mobile_no": lead_data["SENDER_MOBILE"],
                "lead_name": lead_data["SENDER_NAME"],
                "company_name": lead_data.get("SENDER_COMPANY", ""),
                "source": "India Mart",
                "custom_indiamart_id": lead_data["UNIQUE_QUERY_ID"],
                "notes": lead_data.get("QUERY_MESSAGE", "")
            }).insert(ignore_permissions=True)
            return True
        return False
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("Error adding lead from India Mart"))
        return False
