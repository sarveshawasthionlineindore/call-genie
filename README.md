# Call Genie — User Registration & Database Setup Guide

## 1. Strict Registration Flow (Model 1 B2B Standard)
Aapke nirdesh anusaar system me strict registration flow active hai:

1. **Existing Registered Users**:
   - User jab "WhatsApp Login" tab me apna 10-digit registered number dalta hai, system database me check karta hai.
   - Number milne par instant 6-digit WhatsApp OTP dispatch hota hai aur user verify karke seedhe dashboard me enter karta hai.

2. **New / Unregistered Users**:
   - Agar koi naya number jo database me nahi hai, wo WhatsApp Login karne ki koshish karega, to system use block karke saaf batayega:
     `"मोबाइल नंबर +91 [number] अभी रजिस्टर्ड नहीं है। कृपया पहले 'Free Trial' पर क्लिक करके साइन अप करें।"`
   - Frontend automatically us user ko "Free Trial" / Sign Up tab par le jayega aur uska number pre-fill kar dega.
   - Naya user jab apni company details, naam aur password daal kar Free Trial sign up karega, tabhi uska permanent account banega aur use ₹100 Free Calling Credits milenge.

---

## 2. PostgreSQL Permanent Database Connection
Data ko permanent rakhne ke liye Render me PostgreSQL database link connect karein:
- **Variable Name:** `DATABASE_URL`
- **Value:** `postgresql+asyncpg://<username>:<password>@<host>/<database>` (Render internal/external URL)

Is setup ke baad aapka koi bhi registered client ya agent data kabhi delete nahi hoga!
