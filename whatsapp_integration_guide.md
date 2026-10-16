# Call Genie — Client WhatsApp API Onboarding & Integration Guide

This guide details how individual enterprise users and clients configure and connect their own WhatsApp Business API (Meta Cloud API / WhatAPI) to Call Genie for sending automated post-call follow-ups from their own business numbers.

---

## 1. Multi-Tenant Architecture Overview

Call Genie implements a dual-mode **Bring Your Own Key (BYOK)** multi-tenant WhatsApp delivery model:

```
                            ┌─────────────────────────────────────────┐
                            │    Outbound Call Completed by AI        │
                            └────────────────────┬────────────────────┘
                                                 │
                                                 ▼
                            ┌─────────────────────────────────────────┐
                            │  Check Tenant WhatsApp Configuration    │
                            │      (tenant_whatsapp_configs)          │
                            └────────────────────┬────────────────────┘
                                                 │
                        ┌────────────────────────┴────────────────────────┐
                        ▼                                                 ▼
      [Mode: 'managed' / Default]                         [Mode: 'custom' / BYOK]
 ┌──────────────────────────────────────┐        ┌──────────────────────────────────────┐
 │ Uses MyBot Genie Verified Cloud     │        │ Uses Client's Own WhatsApp WABA     │
 │ Gateway (Instant zero-setup).        │        │ Number, Token & Approved Template.   │
 │ Signs off with Client's Brand Name.  │        │ Direct sender branding to customer.  │
 └──────────────────────────────────────┘        └──────────────────────────────────────┘
```

---

## 2. Client Dashboard Setup (Step-by-Step)

Each registered business user has full self-service control over their WhatsApp delivery directly inside their client workspace:

1. **Navigate to Integrations:**
   - Log in to the Call Genie Dashboard (`call.mybotgenie.in/dashboard.html`).
   - Click **Integrations & API** from the left navigation sidebar.

2. **Locate WhatsApp Business API Gateway Card:**
   - The user will see the dedicated **WhatsApp Business API Gateway** section.
   - It offers a two-state selector:
     - **Managed Gateway (Default):** Zero setup. Uses Call Genie's pre-verified Meta gateway.
     - **Connect Custom Meta API:** Enables client to enter their own Meta/WhatAPI credentials.

3. **Enter Credentials (When choosing Custom Meta API):**
   - **WhatsApp Phone Number ID:** Numeric ID from Meta Developer Console (e.g., `93914250482`).
   - **Meta API Gateway URL:** Default is `https://crmapi1.whatapi.in/api/meta` or official Meta Graph URL `https://graph.facebook.com/v19.0`.
   - **Permanent Access Token:** System user bearer token with `whatsapp_business_messaging` permissions.
   - **Approved Template Name:** The pre-approved template registered under the client's WABA (e.g., `kdm_followup_msg`).
   - **Template Language Code:** Language matching the template (e.g., `hi` for Hindi, `en_US` for English, `nl` for Dutch).
   - **Banner Image URL:** Link to the client's business header image/logo.

4. **1-Click Live Verification:**
   - Click the green **"Save Gateway Settings"** button.
   - Click **"Test Send WhatsApp"**, enter any Indian mobile number (`+91`), and send a test message to confirm live delivery.

---

## 3. How Clients Obtain Their WhatsApp API Credentials from Meta

If a client wants to generate their own credentials:

1. Go to [developers.facebook.com](https://www.google.com/search?q=https%3A%2F%2Fdevelopers.facebook.com%2F) and create a Business App.
2. Under **WhatsApp -> API Setup**:
   - Copy the **Phone Number ID**.
3. Under **Business Settings -> System Users**:
   - Create a System User with Admin role.
   - Assign the WhatsApp asset with `Full Control`.
   - Click **Generate New Token**, select `whatsapp_business_messaging` and `whatsapp_business_management`, and set token expiration to **Never**.
4. Under **WhatsApp -> Message Templates**:
   - Submit a utility or marketing template matching the business purpose (e.g., brochure delivery).
   - Once approved by Meta (usually within 2–15 minutes), paste the template name into the Call Genie Dashboard.

---

## 4. Backend Database Schema & API Reference

### Database Model (`tenant_whatsapp_configs`):
- `tenant_id`: Unique identifier for the client's business workspace.
- `mode`: String (`'managed'` or `'custom'`).
- `phone_number_id`: The client's specific WhatsApp Phone Number ID.
- `access_token`: Encrypted/Bearer token for the WhatsApp Gateway.
- `gateway_url`: Target REST endpoint.
- `template_name`: Client-specific Meta approved template.
- `template_lang`: ISO language code.
- `banner_image_url`: Media link for image header.

### API Endpoints:
- `GET /api/integrations/whatsapp` — Fetches current workspace configuration.
- `POST /api/integrations/whatsapp` — Saves or updates configuration.
- `POST /api/integrations/whatsapp/test` — Dispatches a live test template from the client's configured sender.
