# Call Genie — Enterprise Modifications & WhatsApp Multi-Tenant Guide

Aapke dwara maange gaye teeno mukhya modifications ko system me implement kar diya gaya hai:

---

## 1. Bina Question Tag / Bina "Q:" ke Semantic Knowledge Understanding

### Samasya:
Pehle RAG matcher sirf tab achhe se answer de raha tha jab text me saaf `Q:` ya question format ho. Agar koi aam bolchal me puche: *"beej ki rate kya hai"*, *"gehu ka kya rate hai"*, ya *"gold 101 bhav"*, to system confuse ho sakta tha.

### Samadhaan:
1. **Hindi/Hinglish Synonym Engine:**
   - Humne `main.py` ke andar dynamic product & commercial synonym dictionary jod di hai:
     - `beej` ➔ `seed`, `seeds`, `variety`, `hybrid`, `kdm`, `dhan`, `gehu`, `chana`
     - `bhav / daam / kimat / kharcha` ➔ `rate`, `price`, `rs`, `rupaye`, `cost`
     - `gehu` ➔ `wheat`, `sharbati`, `lokwan`
     - `chana` ➔ `gram`, `desi`, `chickpea`
2. **Weighted Semantic Retrieval:**
   - Chahe user question mark (`?`) lagaye ya na lagaye, AI query ke core words aur synonyms ko corpus me dhoondhega.
   - Jis line me product aur currency/rate (`Rs.`, `₹`, `per packet`, `per quintal`) dono honge, AI use sabse high priority dekar clean answer extract karega.
   - Example: *"beej ki rate kya hai"* puchne par wo seedhe KDM Gold-101 ya available beej ki rate bata dega.

---

## 2. Per-Second Billing Pulse (60 Second = ₹5.00, 1 Second = 8.33 Paise)

### Niyam:
* **Rate:** ₹5.00 per minute = 60 seconds ka ₹5.00
* **Per-Second Rate:** ₹5.00 ÷ 60 = **₹0.0833 प्रति सेकंड (8.33 पैसे)**

### Calculation & Implementation:
* **5 Second Call:** `5 × 0.0833` = **₹0.42** (Chalis paise)
* **10 Second Call:** `10 × 0.0833` = **₹0.83**
* **12 Second Call:** `12 × 0.0833` = **₹1.00**
* **30 Second Call:** `30 × 0.0833` = **₹2.50**
* **60 Second Call:** `60 × 0.0833` = **₹5.00**

### Backend & Frontend Sync:
- `place_single_call` aur Exotel Status Callback (`/api/telephony/exotel/callback`) me per-second billing logic active kar diya hai.
- Dashboard par sidebar aur logs me ab exact seconds ke anusaar ₹0.42, ₹1.00 etc. katega aur log table me bhi exact second aur amount dikhega.

---

## 3. Multi-Tenant WhatsApp API Architecture (Clients Apna WhatsApp Kaise Connect Karenge?)

Jab alag-alag clients aapke Call Genie platform par onboard honge, to unke paas 2 options honge:

### Option A: MyBot Genie Managed Cloud Gateway (Zero Setup - Default)
* **Kiske Liye:** Un clients ke liye jinke paas Meta Developer account ya WhatsApp Cloud API nahi hai.
* **Kaise Kaam Karega:**
  - Client ko koi token ya number ID nahi daalna padega.
  - Message MyBot Genie ke verified Meta gateway se jayega, lekin message ke andar **client ki company ka naam** (jaise *KDM Seeds*, *Dr. Sharma Clinic*) aur unka contact auto-insert hoga.
  - Client ke liye bilkul frictionless experience hai.

### Option B: Connect Custom Meta WhatsApp Cloud API / WhatAPI (Client's Own Brand Number)
* **Kiske Liye:** Un enterprise clients ke liye jo chahte hain ki unke khud ke verified WhatsApp number se hi customer ko follow-up message jaye.
* **Client Panel (Dashboard Setup):**
  - Dashboard me **"Integrations & API"** tab me humne ek naya card jod diya hai: **"WhatsApp Business API Gateway"**.
  - Wahan client **"Connect Custom Meta API"** select karega aur ye details bharega:
    1. **WhatsApp Phone Number ID** (Meta Developer Console se)
    2. **Permanent Access Token / Secret Key**
    3. **Approved Template Name** (e.g. `kdm_followup_brochure`)
    4. **Template Language Code** (e.g. `hi` ya `en_US`)
    5. **Banner Image URL** (unka brand banner)
  - **1-Click Test Button:** Client wahi par apna number daal kar **"Test Send WhatsApp"** daba sakta hai, jisse turant verify ho jayega ki unka custom WhatsApp sahi connect hua hai ya nahi.

---

## 4. Deployment Files
1. **GitHub (`call-genie-cloud`):** `main.py`
2. **Hostinger (`public_html/call/`):** `dashboard.html`
