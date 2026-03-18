import io
import zipfile
import pandas as pd
import streamlit as st
import smtplib
from email.message import EmailMessage

st.set_page_config(page_title="Lead Report Splitter", layout="wide")

st.title("Lead Report Splitter + Email Sender")

CC_EMAIL = "erica@livmed.us"


def normalize_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def find_column(df, candidates):
    lower_map = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    for candidate in candidates:
        for col in df.columns:
            if candidate.lower() in str(col).lower():
                return col
    return None


def sanitize_filename(text):
    text = normalize_text(text)
    return "".join(c if c.isalnum() else "_" for c in text)


# =========================
# EMAIL SETTINGS
# =========================
st.sidebar.header("Email Settings")

sender_email = st.sidebar.text_input("Sender Gmail")
app_password = st.sidebar.text_input("App Password", type="password")

test_mode = st.sidebar.checkbox("TEST MODE (send to yourself only)", value=True)
test_email = st.sidebar.text_input("Test Email (your email)")

st.sidebar.info("Emails will ALWAYS CC: erica@livmed.us")

# =========================
# FILE UPLOAD
# =========================
uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])

if uploaded_file:

    xls = pd.ExcelFile(uploaded_file)

    detail_df = pd.read_excel(uploaded_file, sheet_name="Detail")
    conversion_df = pd.read_excel(uploaded_file, sheet_name="Conversion Stats")

    leadsource_col = find_column(detail_df, ["LeadSource"])
    payable_col = find_column(detail_df, ["Payable"])
    paidamount_col = find_column(detail_df, ["PaidAmount"])
    disposition_col = find_column(detail_df, ["Disposition"])

    identifier_col = conversion_df.columns[0]
    center_col = find_column(conversion_df, ["Center Name"])
    email_col = find_column(conversion_df, ["Email"])

    conversion_lookup = pd.DataFrame({
        "Identifier": conversion_df[identifier_col].apply(normalize_text),
        "CenterName": conversion_df[center_col].apply(normalize_text),
        "Email": conversion_df[email_col].apply(normalize_text),
    })

    detail_df["LeadSource_normalized"] = detail_df[leadsource_col].apply(normalize_text)

    merged_df = detail_df.merge(
        conversion_lookup,
        left_on="LeadSource_normalized",
        right_on="Identifier",
        how="left"
    )

    st.subheader("Preview")
    st.dataframe(merged_df.head(), width="stretch")

    # =========================
    # BUILD FILES
    # =========================
    vendor_files = []

    for leadsource, group in merged_df.groupby("LeadSource_normalized"):

        center_name = normalize_text(group["CenterName"].iloc[0])
        email = normalize_text(group["Email"].iloc[0])

        safe_identifier = sanitize_filename(leadsource)
        safe_center = sanitize_filename(center_name) or "UnknownCenter"

        columns_to_remove = [
            "LeadSource_normalized",
            "Identifier",
            "CenterName",
            "Email",
            "PaidAmount",
            "DiabeticOnMedicare",
            "AssignedTo"
        ]

        export_group = group.drop(columns=columns_to_remove, errors="ignore")

        csv_bytes = export_group.to_csv(index=False).encode("utf-8")
        file_name = f"{safe_center}__{safe_identifier}.csv"

        payable_y = 0
        if payable_col:
            payable_series = group[payable_col].astype(str).str.upper()
            payable_y = int((payable_series == "Y").sum())

        vendor_files.append({
            "Center": center_name,
            "Email": email,
            "File": file_name,
            "CSV": csv_bytes,
            "Rows": len(group),
            "Payable": payable_y
        })

    # =========================
    # EMAIL PREVIEW
    # =========================
    st.subheader("Email Preview")

    preview_df = pd.DataFrame(vendor_files)
    st.dataframe(preview_df, width="stretch")

    # =========================
    # SEND EMAIL BUTTON
    # =========================
    if st.button("🚀 SEND EMAILS"):

        if not sender_email or not app_password:
            st.error("Enter Gmail + App Password first")
            st.stop()

        try:
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()
            server.login(sender_email, app_password)

            sent_count = 0

            for item in vendor_files:

                if not item["Email"]:
                    continue

                to_email = test_email if test_mode else item["Email"]

                msg = EmailMessage()
                msg["Subject"] = f"Daily Lead Report - {item['Center']}"
                msg["From"] = sender_email
                msg["To"] = to_email
                msg["CC"] = CC_EMAIL

                msg.set_content(f"""
Hello,

Attached is your daily report.

Total Leads: {item['Rows']}
Payable Leads: {item['Payable']}

Thanks,
LivMed
""")

                msg.add_attachment(
                    item["CSV"],
                    maintype="text",
                    subtype="csv",
                    filename=item["File"]
                )

                server.send_message(msg)
                sent_count += 1

            server.quit()

            st.success(f"✅ Sent {sent_count} emails successfully!")

        except Exception as e:
            st.error(f"Email error: {e}")