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
            else:
                break  # If we didn't get a 429, break out of the retry loop
        
        if res.text:
            response_data = json.loads(res.text)
            
            if isinstance(response_data, dict) and response_data.get("CODE") == 429:
                frappe.msgprint(_("Rate limit exceeded. Please try again later."))
                return
            
            leads_created = 0
            for row in response_data:
                if isinstance(row, dict):
                    if "Error_Message" in row:
                        frappe.throw(row["Error_Message"])
                    else:
                        if add_lead(row):
                            leads_created += 1
            
            if leads_created > 0:
                frappe.msgprint(_("You have successfully added {0} Lead(s)").format(leads_created))
            else:
                frappe.msgprint(_("No new leads were added"))
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("India Mart Sync Error"))
        frappe.throw(_("An error occurred while syncing leads. Please check the error log."))

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

def add_lead(lead_data):
    try:
        if not frappe.db.exists("Lead", {"custom_indiamart_id": lead_data["UNIQUE_QUERY_ID"]}):
            doc = frappe.get_doc(dict{
                "doctype": "Lead",
                "first_name": lead_data["SENDER_NAME"],
                "mobile_no": lead_data["SENDER_MOBILE"],
                "company_name": lead_data.get("SENDER_COMPANY", ""),
                "source": "India Mart",
                "custom_indiamart_id": lead_data["UNIQUE_QUERY_ID"]
            }).insert(ignore_permissions=True)
            return True
        return False
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("Error adding lead from India Mart"))
        return False
