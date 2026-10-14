
    agents = (await db.execute(select(Agent).where(Agent.tenant_id == user.tenant_id))).scalars().all()
    for ag in agents:
        ag.system_prompt = (ag.system_prompt or "") + f"\n\n[MANUAL FACTS ({clean_title})]:\n{clean_content[:8000]}"

    await db.commit()
    await db.refresh(doc)

    return {
        "status": "success",
        "doc_id": doc.id,
        "title": doc.title,
        "char_count": len(clean_content),
        "message": f"Knowledge '{clean_title}' added and AI agent trained!"
    }

@app.delete("/api/knowledge/{doc_id}")
async def delete_knowledge_document(
    doc_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    doc = (await db.execute(
        select(KnowledgeDocument).where(KnowledgeDocument.id == doc_id, KnowledgeDocument.tenant_id == user.tenant_id)
    )).scalar_one_or_none()
    if not doc:
        raise HTTPException(status_code=404, detail="Knowledge document not found.")

    await db.delete(doc)
    await db.commit()
    return {"status": "success", "message": "Knowledge document deleted from AI brain."}
