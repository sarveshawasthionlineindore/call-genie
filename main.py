        user_id=r.user_id,
        recharge_request_id=r.id,
        amount_inr=r.amount_inr,
        gst_rate=0.0,
        gst_amount_inr=0.0,
        total_amount_inr=r.amount_inr,
        payment_method="UPI_QR",
        utr_number=r.utr_number,
        status="PAID"
    )
    db.add(inv)
    await db.commit()
    await db.refresh(inv)

    return {
        "status": "approved",
        "new_balance_inr": round(new_bal, 2),
        "invoice_id": inv.id,
        "invoice_number": inv.invoice_number,
        "amount_inr": inv.amount_inr
    }

@app.post("/api/admin/recharges/{request_id}/reject")
async def admin_reject_recharge(
    request_id: str,
    req: Optional[RechargeRejectRequest] = None,
    admin: User = Depends(get_super_admin_user),
    db: AsyncSession = Depends(get_db)
):
    r = (await db.execute(select(RechargeRequest).where(RechargeRequest.id == request_id))).scalar_one_or_none()
    if not r:
        raise HTTPException(status_code=404, detail="Recharge request not found")
    r.status = "REJECTED"
    if req and req.reason:
        r.admin_notes = req.reason
    await db.commit()
    return {"status": "rejected", "message": "Recharge request rejected"}
