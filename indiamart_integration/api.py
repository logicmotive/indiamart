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
            frappe.msgprint(_("URL Response: {0}").format(res.text))
            response_data = json.loads(res.text)
            
            if isinstance(response_data, dict) and response_data.get("CODE") == 429:
                frappe.msgprint(_("Rate limit exceeded. Please try again later."))
                return
            
            count = 0
            for row in response_data:
                if isinstance(row, dict):
                    if "Error_Message" in row:
                        frappe.throw(row["Error_Message"])
                    else:
                        doc = add_lead(row)
                        if doc:
                            count += 1
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("India Mart Sync Error"))
        raise

@frappe.whitelist()
def add_lead(lead_data):
    try:
        if not frappe.db.exists("Lead", {"custom_indiamart_id": lead_data["UNIQUE_QUERY_ID"]}):
            doc = frappe.get_doc({
                "doctype": "Lead",
                "first_name": lead_data["SENDER_NAME"],
                "email_id": lead_data.get("SENDER_EMAIL", ""),
                "mobile_no": lead_data["SENDER_MOBILE"],
                "job_title": lead_data["UNIQUE_QUERY_ID"],
                "source": "India Mart",
                "custom_indiamart_id": lead_data["UNIQUE_QUERY_ID"]
            }).insert(ignore_permissions=True)
            return doc
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("Error adding lead from India Mart"))
        raise
