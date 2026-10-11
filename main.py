    res = await WhatsAppService.send_post_call_message(
        phone_number=phone_number,
        client_name="Sarvesh",
        business_name="MyBot Genie"
    )
    return res

@app.post("/api/whatsapp/test")
async def test_whatsapp(req: WhatsAppTestRequest):
    """
    Test endpoint to send a live test message to any WhatsApp number.
    """
    res = await WhatsAppService.send_post_call_message(req.phone_number, req.message, client_name="Sarvesh", business_name="MyBot Genie")
    return res

@app.post("/api/demo/personalized-call")
async def personalized_demo(demo_in: PersonalizedDemoRequest, bg: BackgroundTasks):
    clean_phone = demo_in.phone_number.strip().replace(" ", "").replace("-", "")
    bg.add_task(LiveKitService.create_outbound_call, clean_phone, "demo", "demo_tenant", {"system_prompt": "Speak warm Hindi."})
    
    # Auto WhatsApp Follow-up Delivery
    wa_msg = (
        f"नमस्ते {demo_in.client_name} जी! 🙏\n\n"
        f"MyBot Genie की तरफ से Call Genie AI से जुड़ने के लिए बहुत-बहुत धन्यवाद।\n\n"
        f"✨ आपके बिज़नेस '{demo_in.business_name}' ({demo_in.business_type}) के लिए हमारा AI वॉयस कॉलिंग सॉल्यूशन 24/7 तैयार है:\n"
        "📞 100% नेचुरल ह्यूमन वॉइस (हिंदी / हिंग्लिश)\n"
        "🚀 हर इनबाउंड व आउटबाउंड कॉल पर ऑटोमैटिक बातचीत\n"
        "📊 तुरंत लीड क्वालिफिकेशन और अपॉइंटमेंट शेड्यूलिंग\n"
        "💬 कॉल के तुरंत बाद व्हाट्सएप पर ब्रोशर व अपडेट्स\n\n"
        "👉 लाइव डेमो व डिटेल्स देखें: https://call.mybotgenie.in\n\n"
        "— टीम MyBot Genie (Indore)"
    )
    bg.add_task(WhatsAppService.send_post_call_message, clean_phone, wa_msg, client_name=demo_in.client_name, business_name=demo_in.business_name)
    
    return {"status": "initiated", "message": f"Demo call and WhatsApp queued for {clean_phone}"}

@app.post("/api/campaigns/single-call")
async def place_single_call(call_req: SingleCallRequest, user: User = Depends(get_current_user), bg: BackgroundTasks = None, db: AsyncSession = Depends(get_db)):
    clean_phone = call_req.phone_number.strip().replace(" ", "").replace("-", "")
    room = await LiveKitService.create_outbound_call(clean_phone, call_req.agent_id, user.tenant_id, call_req.custom_variables)
    return {"room_name": room, "status": "initiated"}

@app.post("/api/integrations/api-keys")
async def make_key(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    raw = f"cg_live_{secrets.token_urlsafe(32)}"
    key_obj = ApiKey(tenant_id=user.tenant_id, key=raw)
    db.add(key_obj); await db.commit(); await db.refresh(key_obj)
    return {"key": raw}
