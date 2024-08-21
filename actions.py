from db import db

async def exists(table, to_match):
    response = db.table(table).select("*").match(to_match).execute()
    return True if len(response.data) > 0 else False

async def update(table, info, to_match):
    response = db.table(table).update(info).match(to_match).execute()
    return response

async def upload(table, info):
    response = db.table(table).insert(info).execute()
    return response