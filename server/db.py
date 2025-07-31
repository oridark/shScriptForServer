import logging
import mysql.connector
from color import YELLOW, RED

logger = logging.getLogger(__name__)

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def getPtpGroupsFromMysql(table="ptpGroupData",pageSize=1000000,pageNumber=1,groupId=None,title=None,isWebExist=None,sortBy=None,hasGoldenPopcorn=None,sortOrder='DESC'):
    # totalSql = Query.from_(dataTable).select(fn.Count("*").as_("total"))
    # sql = Query.from_(dataTable).select("*")
    sql = f"SELECT * FROM {table} WHERE 1=1"
    totalSql = f"SELECT COUNT(*) as total FROM {table} WHERE 1=1"
    conditions = []
    params = []
    # 添加筛选条件
    if groupId is not None:
        conditions.append("groupId = %s")
        params.append(groupId)
    if title is not None:
        conditions.append("title LIKE %s")
        params.append(f"%{title}%")
    if isWebExist is not None:
        conditions.append("isWebExist = %s")
        params.append(isWebExist)
    if hasGoldenPopcorn is not None:
        conditions.append("hasGoldenPopcorn = %s")
        params.append(hasGoldenPopcorn)
    if conditions:
            sql += " AND " + " AND ".join(conditions)
            totalSql += " AND " + " AND ".join(conditions)
    # Adding sorting
    if sortBy and sortOrder:
        sql += f" ORDER BY {sortBy} {sortOrder}"
    offset = (int(pageNumber) - 1)*int(pageSize)
    sql += f" LIMIT %s OFFSET %s"
    params.extend([pageSize, offset])
    # if groupId is not None:
    #     condition = dataTable.groupId == groupId
    #     sql = sql.where(condition)
    #     totalSql = totalSql.where(condition)
    # if title is not None:
    #     condition = dataTable.title.like(f"%{title}%")
    #     sql = sql.where(condition)
    #     totalSql = totalSql.where(condition)
    # if isWebExist is not None:
    #     condition = dataTable.isWebExist == isWebExist
    #     sql = sql.where(condition)
    #     totalSql = totalSql.where(condition)
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor(dictionary=True)
        # Executing the count query
        cur.execute(totalSql, params[:len(params)-2])  # Removing limit and offset parameters for count query
        total = cur.fetchone()
        # Executing the main query
        cur.execute(sql, params)
        list = cur.fetchall()
        return {
            "total": total["total"],
            "list": list
        }
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def getPtpGroupsNotInMysql(table="ptpGroupData", groupIds = []):
    sql = """
        SELECT groupId
        FROM ptpGroupData
        WHERE groupId IN (%s)
        """ % ','.join(['%s'] * len(groupIds))
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        # Executing the main query
        cur.execute(sql, tuple(groupIds))
        # 获取结果
        existing_group_ids = cur.fetchall()
        # 提取存在的 groupId
        existing_group_ids_set = {row[0] for row in existing_group_ids}
        # 找出不存在的 groupId
        non_existing_group_ids = [group_id for group_id in groupIds if group_id not in existing_group_ids_set]
        return {
            "notExistedIds": non_existing_group_ids,
            "existedIds": existing_group_ids_set
        }
    except Exception as e:
        print('getPtpGroupsNotInMysql Exception', e)
        return []
    finally:
        if conn:
            conn.close()

def getPtpGroupsWithImdb(table="ptpGroupData",pageSize=1000000,pageNumber=1):
    sql = f"SELECT * FROM {table} WHERE ImdbId IS NOT NULL AND ImdbId != '' AND tmdbId IS NULL"
    totalSql = f"SELECT COUNT(*) as total FROM {table} WHERE ImdbId IS NOT NULL AND ImdbId != '' AND tmdbId IS NULL"
    params = []
    sql += f" ORDER BY ImdbId ASC"
    offset = (int(pageNumber) - 1)*int(pageSize)
    sql += f" LIMIT %s OFFSET %s"
    params.extend([pageSize, offset])
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor(dictionary=True)
        # Executing the count query
        cur.execute(totalSql, params[:len(params)-2])  # Removing limit and offset parameters for count query
        total = cur.fetchone()
        # Executing the main query
        cur.execute(sql, params)
        list = cur.fetchall()
        return {
            "total": total["total"],
            "list": list
        }
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def getPtpGroupsWithTmdb(table="ptpGroupData",pageSize=1000000,pageNumber=1):
    sql = f"SELECT * FROM {table} WHERE tmdbId IS NOT NULL and tmdbId != '' and country != 'China' and country != 'Hong Kong' and country is not NULL"
    totalSql = f"SELECT COUNT(*) as total FROM {table} WHERE tmdbId IS NOT NULL and tmdbId != '' and country != 'China' and country != 'Hong Kong' and country is not NULL"
    params = []
    sql += f" ORDER BY groupId ASC"
    offset = (int(pageNumber) - 1)*int(pageSize)
    sql += f" LIMIT %s OFFSET %s"
    params.extend([pageSize, offset])
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor(dictionary=True)
        # Executing the count query
        cur.execute(totalSql, params[:len(params)-2])  # Removing limit and offset parameters for count query
        total = cur.fetchone()
        # Executing the main query
        cur.execute(sql, params)
        list = cur.fetchall()
        return {
            "total": total["total"],
            "list": list
        }
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def getPtpGroupsWithoutImdb(table="ptpGroupData",pageSize=1000000,pageNumber=1):
    sql = f"SELECT * FROM {table} WHERE ImdbId IS NULL or ImdbId = ''"
    totalSql = f"SELECT COUNT(*) as total FROM {table} WHERE ImdbId IS NULL or ImdbId = ''"
    params = []
    sql += f" ORDER BY groupId ASC"
    offset = (int(pageNumber) - 1)*int(pageSize)
    sql += f" LIMIT %s OFFSET %s"
    params.extend([pageSize, offset])
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor(dictionary=True)
        # Executing the count query
        cur.execute(totalSql, params[:len(params)-2])  # Removing limit and offset parameters for count query
        total = cur.fetchone()
        # Executing the main query
        cur.execute(sql, params)
        list = cur.fetchall()
        return {
            "total": total["total"],
            "list": list
        }
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def insertPtpGroupToMysql(data, table="ptpGroupData",):
    sql_query = """INSERT INTO ptpGroupData (groupId, title, groupInfo, isWebExist, initYear, isMkvExist, isBDExist, size, torrentNumber, BDSeederNumber, ImdbId, ImdbRating, ImdbVoteCount,totalSeedNumber, hasGoldenPopcorn) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.executemany(sql_query, data)
        conn.commit()
        logger.info(f"ptpGroup Insert")
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def getBluTorrentsFromMysql(pageSize=1000000,pageNumber=1,torrentId=None,name=None,isDownloaded=None,isUploaded=None,ImdbId=None,uploadSiteTorrentId=None,uploadSiteUrl=None,sortBy='torrentId',tmdbId=None,sortOrder='DESC'):
    sql = """
        SELECT blu.*, upload.isUploaded, upload.uploadSiteTorrentId, upload.uploadSiteUrl 
        FROM bluTorrentData1 blu
        LEFT JOIN bluTorrentUploadStatus upload ON blu.torrentId = upload.torrentId
        WHERE 1=1
    """
    totalSql = """
        SELECT COUNT(*) as total 
        FROM bluTorrentData1 blu
        LEFT JOIN bluTorrentUploadStatus upload ON blu.torrentId = upload.torrentId
        WHERE 1=1
    """
    conditions = []
    params = []
    # 添加筛选条件
    if torrentId is not None:
        conditions.append("blu.torrentId = %s")
        params.append(torrentId)
    if name is not None:
        conditions.append("blu.name LIKE %s")
        params.append(f"%{name}%")
    if isDownloaded is not None:
        conditions.append("blu.isDownloaded = %s")
        params.append(isDownloaded)
    if isUploaded is not None:
        if isUploaded == 0 or isUploaded == '0':
            conditions.append("(upload.isUploaded = %s or upload.isUploaded is NULL)")
        else:
            conditions.append("upload.isUploaded = %s")
        params.append(isUploaded)
    if uploadSiteTorrentId is not None:
        conditions.append("upload.uploadSiteTorrentId = %s")
        params.append(isUploaded)
    if uploadSiteUrl is not None:
        conditions.append("upload.uploadSiteUrl = %s")
        params.append(isUploaded)
    if ImdbId is not None:
        conditions.append("blu.ImdbId = %s")
        params.append(ImdbId)
    if tmdbId is not None:
        conditions.append("blu.tmdbId = %s")
        params.append(tmdbId)
    if conditions:
            sql += " AND " + " AND ".join(conditions)
            totalSql += " AND " + " AND ".join(conditions)
    # Adding sorting
    if sortBy and sortOrder:
        sql += f" ORDER BY blu.{sortBy} {sortOrder}"
    offset = (int(pageNumber) - 1)*int(pageSize)
    sql += f" LIMIT %s OFFSET %s"
    params.extend([pageSize, offset])
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor(dictionary=True)
        # Executing the count query
        cur.execute(totalSql, params[:len(params)-2])  # Removing limit and offset parameters for count query
        total = cur.fetchone()
        # Executing the main query
        cur.execute(sql, params)
        list = cur.fetchall()
        return {
            "total": total["total"],
            "list": list
        }
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def getSourceSiteTorrentsFromMysql(pageSize=1000000,pageNumber=1,torrentId=None,name=None,isDownloaded=None,isUploaded=None,ImdbId=None,uploadSiteTorrentId=None,uploadSiteUrl=None,sortBy='torrentId',tmdbId=None,sortOrder='DESC'):
    sql = """
        SELECT blu.*, upload.isUploaded, upload.uploadSiteTorrentId, upload.uploadSiteUrl 
        FROM sourceTorrentData blu
        LEFT JOIN sourceTorrentUploadStatus upload ON blu.torrentId = upload.torrentId
        WHERE 1=1
    """
    totalSql = """
        SELECT COUNT(*) as total 
        FROM sourceTorrentData blu
        LEFT JOIN sourceTorrentUploadStatus upload ON blu.torrentId = upload.torrentId
        WHERE 1=1
    """
    conditions = []
    params = []
    # 添加筛选条件
    if torrentId is not None:
        conditions.append("blu.torrentId = %s")
        params.append(torrentId)
    if name is not None:
        conditions.append("blu.name LIKE %s")
        params.append(f"%{name}%")
    if isDownloaded is not None:
        conditions.append("blu.isDownloaded = %s")
        params.append(isDownloaded)
    if isUploaded is not None:
        if isUploaded == 0 or isUploaded == '0':
            conditions.append("(upload.isUploaded = %s or upload.isUploaded is NULL)")
        else:
            conditions.append("upload.isUploaded = %s")
        params.append(isUploaded)
    if uploadSiteTorrentId is not None:
        conditions.append("upload.uploadSiteTorrentId = %s")
        params.append(isUploaded)
    if uploadSiteUrl is not None:
        conditions.append("upload.uploadSiteUrl = %s")
        params.append(isUploaded)
    if ImdbId is not None:
        conditions.append("blu.ImdbId = %s")
        params.append(ImdbId)
    if tmdbId is not None:
        conditions.append("blu.tmdbId = %s")
        params.append(tmdbId)
    if conditions:
            sql += " AND " + " AND ".join(conditions)
            totalSql += " AND " + " AND ".join(conditions)
    # Adding sorting
    if sortBy and sortOrder:
        sql += f" ORDER BY blu.{sortBy} {sortOrder}"
    offset = (int(pageNumber) - 1)*int(pageSize)
    sql += f" LIMIT %s OFFSET %s"
    params.extend([pageSize, offset])
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData',
        )
        cur = conn.cursor(dictionary=True)
        # Executing the count query
        cur.execute(totalSql, params[:len(params)-2])  # Removing limit and offset parameters for count query
        total = cur.fetchone()
        # Executing the main query
        cur.execute(sql, params)
        list = cur.fetchall()
        return {
            "total": total["total"],
            "list": list
        }
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def insertBluTorrentToMysql(data):
    sql_query = """INSERT INTO bluTorrentData (torrentId, name, bdInfo, mediainfo, description, categoryid, typeId, resolutionId, regionId, distributorId, size, ImdbId, tmdbId, seeders, releaseYear, resolution, genres, poster) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.executemany(sql_query, data)
        conn.commit()
        logger.info(f"blu torrent Insert")
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def insertBluTorrentStatusToMysql(data):
    sql_query = """INSERT INTO bluTorrentUploadStatus (torrentId, name, isUploaded, uploadSiteTorrentId, uploadSiteUrl) VALUES (%s, %s, %s, %s, %s)"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.executemany(sql_query, data)
        conn.commit()
        logger.info(f"blu torrent status Insert")
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def insertSourceSiteTorrentStatusToMysql(data):
    sql_query = """INSERT INTO sourceTorrentUploadStatus (torrentId, sourceId, name, isUploaded, uploadSiteTorrentId, uploadSiteUrl) VALUES (%s, %s, %s, %s, %s, %s)"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.executemany(sql_query, data)
        conn.commit()
        logger.info(f"SourceSite torrent status Insert")
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()


def batchUpdateBluTorrentsInfoWithMysql(updates, tableName='bluTorrentData', batch_size=1000):
    conn = None
    try:
        # 连接到数据库
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()

        # 修改1: 分批处理数据更新
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            
            for update in batch:
                if not update.get('torrentId'):
                    print(f"跳过没有 torrentId 的记录: {update}")
                    continue
                
                torrentId = update['torrentId']
                
                # 动态生成 SET 子句
                set_clause = ", ".join(
                    f"{field} = %s"
                    for field in update.keys()
                    if field != 'torrentId'
                )
                if not set_clause:
                    print(f"记录 {torrentId} 没有需要更新的字段，跳过。")
                    continue
                
                # 构建更新查询
                final_query = f"UPDATE {tableName} SET {set_clause} WHERE torrentId = %s"
                update_data = [value for field, value in update.items() if field != 'torrentId'] + [torrentId]
                print(f"Executing SQL: {final_query}")
                print(f"With Data: {update_data}")
                try:
                    # 修改2: 加入 try-except 捕获执行异常
                    cur.execute(final_query, tuple(update_data))
                except mysql.connector.Error as e:
                    print(f"更新异常: {e}")
                    conn.rollback()  # 修改3: 出现异常时回滚
                    continue

            # 修改4: 提交每个批次的更新
            conn.commit()
            print(f"批次更新完成：{i // batch_size + 1}/{(len(updates) + batch_size - 1) // batch_size}")
        
        print("所有批量更新完成。")
    except Exception as e:
        print(f"异常: {e}")
    finally:
        if conn:
            conn.close()

def batchUpdatePtpGroupInfoWithMysql(updates, tableName='ptpGroupData', batch_size=1000):
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            
            for update in batch:
                if not update.get('groupId'):
                    print(f"跳过没有 groupId 的记录: {update}")
                    continue
                
                group_id = update['groupId']
                # 动态生成 SET 子句
                set_clause = ", ".join(
                    f"{field} = %s"
                    for field in update.keys()
                    if field != 'groupId'
                )
                if not set_clause:
                    print(f"记录 {group_id} 没有需要更新的字段，跳过。")
                    continue
                
                # 构建更新查询
                final_query = f"UPDATE {tableName} SET {set_clause} WHERE groupId = %s"
                update_data = [value for field, value in update.items() if field != 'groupId'] + [group_id]
                
                try:
                    cur.execute(final_query, tuple(update_data))
                except mysql.connector.Error as e:
                    print(f"更新异常: {e}")
                    conn.rollback()
                    continue
            conn.commit()
            print(f"批次更新完成：{i // batch_size + 1}/{(len(updates) + batch_size - 1) // batch_size}")
        print("所有批量更新完成。")
    except Exception as e:
        print(f"异常: {e}")
    finally:
        if conn:
            conn.close()

def insertPtpGroupToMysqlInRaw(data):
    sql_query = """INSERT INTO ptpGroupRawData (groupId, groupInfo) VALUES (%s, %s)"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.executemany(sql_query, data)
        conn.commit()
        logger.info(f"ptpGroup Insert")
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def insertPtpGroupTorrentFileInfoToMysql(data):
    sql_query = """INSERT INTO ptpGroupTorrentFileStatus (groupId, torrentId, size, seedNumber, Container, isGoldPopcorn) VALUES (%s, %s, %s, %s, %s, %s)"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.executemany(sql_query, data)
        conn.commit()
        logger.info(f"ptpGroupTorrentFileStatus Insert")
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def batchUpdatePtpGroupInfoWithMysqlInRaw(updates, batch_size=100):
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            
            for update in batch:
                if not update.get('groupId'):
                    print(f"跳过没有 groupId 的记录: {update}")
                    continue
                
                group_id = update['groupId']
                # 动态生成 SET 子句
                set_clause = ", ".join(
                    f"{field} = %s"
                    for field in update.keys()
                    if field != 'groupId'
                )
                if not set_clause:
                    print(f"记录 {group_id} 没有需要更新的字段，跳过。")
                    continue
                
                # 构建更新查询
                final_query = f"UPDATE ptpGroupRawData SET {set_clause} WHERE groupId = %s"
                update_data = [value for field, value in update.items() if field != 'groupId'] + [group_id]
                
                try:
                    cur.execute(final_query, tuple(update_data))
                    print(f"批次更新完成：{i // batch_size + 1}/{(len(updates) + batch_size - 1) // batch_size}")
                except mysql.connector.Error as e:
                    print(f"更新异常: {e}")
                    print("batch:", batch)
                    print("SQL Query:", final_query)
                    print("Update Data:", tuple(update_data))
                    conn.rollback()
                    continue
            conn.commit()
        print("所有批量更新完成。")
    except Exception as e:
        print(f"异常: {e}")
    finally:
        if conn:
            conn.close()

def updatePtpGroupWithMysql(groupId, groupInfo=None, tmdbId=None, tmdbRating=None, tmdbVoteCount=None, tmdbTitle=None, ImdbId=None, ImdbRating=None, ImdbVoteCount=None, country=None):
    sql = "UPDATE ptpGroupData SET "
    params = []
    if tmdbId is not None:
        sql += "tmdbId = %s, "
        params.append(tmdbId)
    if tmdbRating is not None:
        sql += "tmdbRating = %s, "
        params.append(tmdbRating)
    if tmdbVoteCount is not None:
        sql += "tmdbVoteCount = %s, "
        params.append(tmdbVoteCount)
    if tmdbTitle is not None:
        sql += "tmdbTitle = %s, "
        params.append(tmdbTitle)
    if ImdbId is not None:
        sql += "ImdbId = %s, "
        params.append(ImdbId)
    if ImdbRating is not None:
        sql += "ImdbRating = %s, "
        params.append(ImdbRating)
    if ImdbVoteCount is not None:
        sql += "ImdbVoteCount = %s, "
        params.append(ImdbVoteCount)
    if country is not None:
        sql += "country = %s, "
        params.append(country)
    if groupInfo is not None:
        sql += "groupInfo = %s, "
        params.append(groupInfo)
    # Remove trailing comma and space
    sql = sql.rstrip(", ")
    # Add WHERE clause
    sql += " WHERE groupId = %s"
    params.append(groupId)
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        conn.commit()
        print("Update successful")
    except Exception as e:
        print('updatePtpGroupData Exception:', e)
    finally:
        if conn:
            conn.close()

def batchUpdatePtpGroupImdbIdWithMysql(updates, batch_size=1000):
    update_query = """
        UPDATE ptpGroupData
        SET hasGoldenPopcorn = CASE groupId
            {0}
            END
        WHERE groupId IN ({1})
    """
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            print(i+1)
            set_hasGoldenPopcorn = []
            group_ids = []

            for update in batch:
                group_ids.append(update['groupId'])
                set_hasGoldenPopcorn.append(f"WHEN {update['groupId']} THEN %s")

            final_query = update_query.format(
                " ".join(set_hasGoldenPopcorn),
                ", ".join(map(str, group_ids))
            )

            update_data = []
            for update in batch:
                update_data.append(update.get('hasGoldenPopcorn', None))

            try:
                cur.execute(final_query, update_data)
                conn.commit()
            except mysql.connector.Error as e:
                print(f"批量更新异常: {e}")
                conn.rollback()
                
        print("所有批量更新完成。")
    except Exception as e:
        print('updatePtpGroupData Exception:', e)
    finally:
        if conn:
            conn.close()

def batchUpdatePtpGroupWithMysql(updates):
    update_query = """
        UPDATE ptpGroupData
        SET ImdbId = CASE groupId
            {0}
            END,
            ImdbRating = CASE groupId
            {1}
            END,
            ImdbVoteCount = CASE groupId
            {2}
            END
        WHERE groupId IN ({3})
    """
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        
        set_imdbid = []
        set_imdbrating = []
        set_imdbvotecount = []
        group_ids = []

        for update in updates:
            group_ids.append(update['groupId'])
            set_imdbid.append(f"WHEN {update['groupId']} THEN %s")
            set_imdbrating.append(f"WHEN {update['groupId']} THEN %s")
            set_imdbvotecount.append(f"WHEN {update['groupId']} THEN %s")

        final_query = update_query.format(
            " ".join(set_imdbid),
            " ".join(set_imdbrating),
            " ".join(set_imdbvotecount),
            ", ".join(map(str, group_ids))
        )
        update_data = []
        for update in updates:
            update_data.extend([update.get('ImdbId', None), update.get('ImdbRating', None), update.get('ImdbVoteCount', None)])

        cur.execute(final_query, update_data)
        conn.commit()
        print("批量更新完成。")
    except Exception as e:
        print('updatePtpGroupData Exception:', e)
    finally:
        if conn:
            conn.close()


def insertAlbumsForRedToMysql(data):
    sql_query = """INSERT INTO albumStatusForPtSiteForRed (qobuzId, uploadedStatus) VALUES (%s, %s)"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.executemany(sql_query, data)
        conn.commit()
        logger.info(f"albumsForRed torrent Insert")
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def insertAlbumsForOpsToMysql(data):
    sql_query = """INSERT INTO albumStatusForPtSiteForOps (qobuzId, uploadedStatus) VALUES (%s, %s)"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.executemany(sql_query, data)
        conn.commit()
        logger.info(f"albumsForOps torrent Insert")
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def getAlbumsFromMysql(orderType=None, pageSize=1000000,pageNumber=1,qobuzId=None,albumTitle=None,isHiRes=None,downloadStatusFor16bit=None,downloadStatusFor24bit=None,albumListForMp3320=None):
    sql = f"""SELECT * FROM albumsFromQobuz where 1=1"""
    totalSql = f"""SELECT count(*) as total FROM albumsFromQobuz where 1=1"""
    conditions = []
    params = []
    # 添加筛选条件
    if qobuzId is not None:
        conditions.append("qobuzId = %s")
        params.append(qobuzId)
    if albumTitle is not None:
        conditions.append("albumTitle LIKE %s")
        params.append(f"%{albumTitle}%")
    if downloadStatusFor16bit is not None:
        conditions.append("downloadStatusFor16bit = %s")
        params.append(downloadStatusFor16bit)
    if downloadStatusFor24bit is not None:
        conditions.append("downloadStatusFor24bit = %s")
        params.append(downloadStatusFor24bit)
    if albumListForMp3320 is not None:
        conditions.append("albumListForMp3320 = %s")
        params.append(albumListForMp3320)
    if isHiRes is not None:
        conditions.append("isHiRes = %s")
        params.append(isHiRes)
    if conditions:
            sql += " AND " + " AND ".join(conditions)
            totalSql += " AND " + " AND ".join(conditions)
    if orderType == '1':
        sql += f" ORDER BY trackNumber desc"
    elif orderType == '2':
        sql += f" ORDER BY trackNumber asc"
    elif orderType == '3':
        sql += f" ORDER BY releaseTime asc"
    else:
        sql += f" ORDER BY releaseTime desc"
    offset = (int(pageNumber) - 1)*int(pageSize)
    sql += f" LIMIT %s OFFSET %s"
    params.extend([int(pageSize), offset])
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor(dictionary=True)
        # Executing the count query
        cur.execute(totalSql, params[:len(params)-2])  # Removing limit and offset parameters for count query
        total = cur.fetchone()
        # Executing the main query
        cur.execute(sql, params)
        list = cur.fetchall()
        return {
            "total": total["total"],
            "list": list
        }
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def insertAllAlbumsToMysqlFromSqlite(data):
    # "qobuzId","albumInfo","albumTitle","releaseTime","isHiRes","downloadStatusFor16bit","downloadStatusFor24bit","downloadStatusForMp3320","trackNumber"
    sql_query = """INSERT INTO albumsFromQobuz (qobuzId,albumInfo,albumTitle,isHiRes,trackNumber,downloadStatusFor16bit,downloadStatusFor24bit,downloadStatusForMp3320,downloadStartTime,fileDirFor24bit,fileDirFor16bit,fileDirForMp3320,coverUrl,coverUrlTranslate,spectrogramUrlFor24bit,spectrogramUrlFor16bit,spectrogramUrlForMp3320,releaseTime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.executemany(sql_query, data)
        conn.commit()
        logger.info(f"albums torrent Insert")
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def insertAlbumsToMysql(data):
    # "qobuzId","albumInfo","albumTitle","releaseTime","isHiRes","downloadStatusFor16bit","downloadStatusFor24bit","downloadStatusForMp3320","trackNumber"
    sql_query = """INSERT INTO albumsFromQobuz (qobuzId,albumInfo,albumTitle,releaseTime,isHiRes,downloadStatusFor16bit,downloadStatusFor24bit,downloadStatusForMp3320,trackNumber) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.executemany(sql_query, data)
        conn.commit()
        logger.info(f"albums torrent Insert")
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def deleteAlbumsToMysql(qobuzId):
    sql_album_query = """delete from albumsFromQobuz where qobuzId = %s"""
    sql_album_status_query1 = """delete from albumStatusForPtSiteForRed where qobuzId = %s"""
    sql_album_status_query2 = """delete from albumStatusForPtSiteForOps where qobuzId = %s"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.execute(sql_album_query, (qobuzId,))
        cur.execute(sql_album_status_query1, (qobuzId,))
        cur.execute(sql_album_status_query2, (qobuzId,))
        conn.commit()
        logger.info(f"albums torrent Insert")
    except Exception as e:
        print(e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def batchUpdateAlbumInfoWithMysql(updates=[], batch_size=1000):
    conn = None
    try:
        # 连接到数据库
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()

        # 修改1: 分批处理数据更新
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            
            for update in batch:
                if not update.get('qobuzId'):
                    print(f"跳过没有 qobuzId 的记录: {update}")
                    continue
                
                qobuzId = update['qobuzId']
                
                # 动态生成 SET 子句
                set_clause = ", ".join(
                    f"{field} = %s"
                    for field in update.keys()
                    if field != 'qobuzId'
                )
                if not set_clause:
                    print(f"记录 {qobuzId} 没有需要更新的字段，跳过。")
                    continue
                
                # 构建更新查询
                final_query = f"UPDATE albumsFromQobuz SET {set_clause} WHERE qobuzId = %s"
                update_data = [value for field, value in update.items() if field != 'qobuzId'] + [qobuzId]
                try:
                    # 修改2: 加入 try-except 捕获执行异常
                    cur.execute(final_query, tuple(update_data))
                except mysql.connector.Error as e:
                    print(f"更新异常: {e}")
                    conn.rollback()  # 修改3: 出现异常时回滚
                    continue

            # 修改4: 提交每个批次的更新
            conn.commit()
            print(f"批次更新完成：{i // batch_size + 1}/{(len(updates) + batch_size - 1) // batch_size}")
        
        print("所有批量更新完成。")
    except Exception as e:
        print(f"异常: {e}")
    finally:
        if conn:
            conn.close()

def getAlbumsWithStatusFromMysql(ptType=None, orderType=None, sidePtType=None, pageSize=1000000,pageNumber=1,qobuzId=None,uploadedStatus=None,albumTitle=None,isHiRes=None,trackNumberMax=None,trackNumberMin=None,downloadStatusFor16bit=None,downloadStatusFor24bit=None):
    tableName = 'albumStatusForPtSiteForRed'
    if ptType is not None:
        tableName = 'albumStatusForPtSiteFor' + ptType
    sql = f"""
        SELECT albums.*, status.uploadedStatus, status.uploadByUser, status.canUploadedBy
        FROM albumsFromQobuz albums
        LEFT JOIN {tableName} status ON albums.qobuzId = status.qobuzId where 1=1"""
    totalSql = f"""
        SELECT COUNT(*) as total 
        FROM albumsFromQobuz albums
        LEFT JOIN {tableName} status ON albums.qobuzId = status.qobuzId where 1=1"""
    conditions = []
    params = []
    # 添加筛选条件
    if qobuzId is not None:
        conditions.append("albums.qobuzId = %s")
        params.append(qobuzId)
    if albumTitle is not None:
        conditions.append("albums.albumTitle LIKE %s")
        params.append(f"%{albumTitle}%")
    if downloadStatusFor16bit is not None:
        conditions.append("albums.downloadStatusFor16bit = %s")
        params.append(downloadStatusFor16bit)
    if downloadStatusFor24bit is not None:
        conditions.append("albums.downloadStatusFor24bit = %s")
        params.append(downloadStatusFor24bit)
    if uploadedStatus is not None:
        conditions.append("status.uploadedStatus = %s")
        params.append(uploadedStatus)
    if isHiRes is not None:
        conditions.append("albums.isHiRes = %s")
        params.append(isHiRes)
    if sidePtType != None:
        totalSql += f" and albums.qobuzId in (select qobuzId from albumStatusForPtSiteForOps where albumStatusForPtSiteForOps.uploadedStatus=2)"
        sql += f" and albums.qobuzId in (select qobuzId from albumStatusForPtSiteForOps where albumStatusForPtSiteForOps.uploadedStatus=2)"
    if conditions:
            sql += " AND " + " AND ".join(conditions)
            totalSql += " AND " + " AND ".join(conditions)
    if orderType == '1':
        sql += f" ORDER BY albums.trackNumber desc"
    elif orderType == '2':
        sql += f" ORDER BY albums.trackNumber asc"
    elif orderType == '3':
        sql += f" ORDER BY albums.releaseTime asc"
    else:
        sql += f" ORDER BY albums.releaseTime desc"
    offset = (int(pageNumber) - 1)*int(pageSize)
    sql += f" LIMIT %s OFFSET %s"
    params.extend([int(pageSize), offset])
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor(dictionary=True)
        # Executing the count query
        cur.execute(totalSql, params[:len(params)-2])  # Removing limit and offset parameters for count query
        total = cur.fetchone()
        # Executing the main query
        cur.execute(sql, params)
        list = cur.fetchall()
        return {
            "total": total["total"],
            "list": list
        }
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def getUncheckedAlbumsFromMysql(ptType=None):
    tableName = 'albumStatusForPtSiteForRed'
    if ptType is not None:
        tableName = 'albumStatusForPtSiteFor' + ptType
    sql = f"""SELECT * FROM albumsFromQobuz where albumsFromQobuz.qobuzId not in (select qobuzId from {tableName})"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor(dictionary=True)
        # Executing the main query
        cur.execute(sql)
        list = cur.fetchall()
        return list
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def batchUpdateAlbumStatusInfoWithMysql(updates=[], tableName='albumStatusForPtSiteForRed', batch_size=1000):
    conn = None
    try:
        # 连接到数据库
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()

        # 修改1: 分批处理数据更新
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            
            for update in batch:
                if not update.get('qobuzId'):
                    print(f"跳过没有 qobuzId 的记录: {update}")
                    continue
                
                qobuzId = update['qobuzId']
                
                # 动态生成 SET 子句
                set_clause = ", ".join(
                    f"{field} = %s"
                    for field in update.keys()
                    if field != 'qobuzId'
                )
                if not set_clause:
                    print(f"记录 {qobuzId} 没有需要更新的字段，跳过。")
                    continue
                
                # 构建更新查询
                final_query = f"UPDATE {tableName} SET {set_clause} WHERE qobuzId = %s"
                update_data = [value for field, value in update.items() if field != 'qobuzId'] + [qobuzId]
                print(f"Executing SQL: {final_query}")
                print(f"With Data: {update_data}")
                try:
                    # 修改2: 加入 try-except 捕获执行异常
                    cur.execute(final_query, tuple(update_data))
                except mysql.connector.Error as e:
                    print(f"更新异常: {e}")
                    conn.rollback()  # 修改3: 出现异常时回滚
                    continue

            # 修改4: 提交每个批次的更新
            conn.commit()
            print(f"批次更新完成：{i // batch_size + 1}/{(len(updates) + batch_size - 1) // batch_size}")
        
        print("所有批量更新完成。")
    except Exception as e:
        print(f"异常: {e}")
    finally:
        if conn:
            conn.close()

def batchInsertAlbumStatusToMysql(data=[], tableName='albumStatusForPtSiteForRed'):
    sql_query = f"""INSERT INTO {tableName} (qobuzId, uploadedStatus) VALUES (%s, %s)"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.executemany(sql_query, data)
        conn.commit()
        logger.info(f"albumsForRed torrent Insert")
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def getAllBluTorrentsFromMysql(pageSize=1000000,pageNumber=1,sortBy=None,sortOrder='DESC'):
    sql = """
        SELECT btd.*
        FROM bluTorrentData1 btd
        LEFT JOIN bluTorrentUploadStatus bus ON btd.torrentId = bus.torrentId
        WHERE bus.torrentId IS NULL and btd.isDownloaded=1
    """
    totalSql = """
        SELECT COUNT(*) as total 
        FROM bluTorrentData1 btd
        LEFT JOIN bluTorrentUploadStatus bus ON btd.torrentId = bus.torrentId
        WHERE bus.torrentId IS NULL and btd.isDownloaded=1
    """
    params = []
    if sortBy and sortOrder:
        sql += f" ORDER BY btd.{sortBy} {sortOrder}"
    offset = (int(pageNumber) - 1)*int(pageSize)
    sql += f" LIMIT %s OFFSET %s"
    params.extend([pageSize, offset])
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor(dictionary=True)
        # Executing the count query
        cur.execute(totalSql, params[:len(params)-2])  # Removing limit and offset parameters for count query
        total = cur.fetchone()
        # Executing the main query
        cur.execute(sql, params)
        list = cur.fetchall()
        return {
            "total": total["total"],
            "list": list
        }
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def getAllBluTorrentsFromMysql2(pageSize=1000000,pageNumber=1,sortBy=None,sortOrder=None):
    sql = """
        SELECT btd.*
        FROM bluTorrentData btd
        LEFT JOIN bluTorrentUploadStatus bus ON btd.torrentId = bus.torrentId
        WHERE bus.torrentId IS NULL and btd.isDownloaded=1 and btd.resolutionId in (1,2,3) and btd.seeders >= 5 and btd.seeders < 50 and btd.releaseYear < 2014 and btd.size > 1000000000 and btd.size < 30720000000
    """
    totalSql = """
        SELECT COUNT(*) as total 
        FROM bluTorrentData btd
        LEFT JOIN bluTorrentUploadStatus bus ON btd.torrentId = bus.torrentId
        WHERE bus.torrentId IS NULL and btd.isDownloaded=1 and btd.resolutionId in (1,2,3) and btd.seeders >= 5 and btd.seeders < 50 and btd.releaseYear < 2014 and btd.size > 1000000000 and btd.size < 30720000000
    """
    params = []
    if sortBy and sortOrder:
        sql += f" ORDER BY btd.{sortBy} {sortOrder}"
    offset = (int(pageNumber) - 1)*int(pageSize)
    sql += f" LIMIT %s OFFSET %s"
    params.extend([pageSize, offset])
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor(dictionary=True)
        # Executing the count query
        cur.execute(totalSql, params[:len(params)-2])  # Removing limit and offset parameters for count query
        total = cur.fetchone()
        # Executing the main query
        cur.execute(sql, params)
        list = cur.fetchall()
        return {
            "total": total["total"],
            "list": list
        }
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def insertSourceTorrentToMysql(data):
    sql_query = """INSERT INTO sourceTorrentData (torrentId, name, sourceSite, bdInfo, mediainfo, description, categoryid, typeId, resolutionId, regionId, distributorId, size, ImdbId, tmdbId, seeders, releaseYear, resolution, genres, poster) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.executemany(sql_query, data)
        conn.commit()
        logger.info(f"source torrent Insert")
    except Exception as e:
        print(e)
    finally:
        if conn:
            conn.close()

def deleteRedStatusAlbumFromMysql(qobuzId):
    sql_album_status_query = """delete from albumStatusForPtSiteForRed where qobuzId = %s"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.execute(sql_album_status_query, (qobuzId,))
        conn.commit()
        logger.info(f"albums torrent Insert")
    except Exception as e:
        print(e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def deleteOpsStatusAlbumFromMysql(qobuzId):
    sql_album_status_query = """delete from albumStatusForPtSiteForOps where qobuzId = %s"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.execute(sql_album_status_query, (qobuzId,))
        conn.commit()
        logger.info(f"albums torrent Insert")
    except Exception as e:
        print(e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def deleteAlbumsToMysql(qobuzId):
    sql_album_query = """delete from albumsFromQobuz where qobuzId = %s"""
    sql_album_status_query1 = """delete from albumStatusForPtSiteForRed where qobuzId = %s"""
    sql_album_status_query2 = """delete from albumStatusForPtSiteForOps where qobuzId = %s"""
    conn = None
    try:
        conn = mysql.connector.connect(
            host='mysql.movietosee.top',
            port='48306',
            user='tjc',
            password='13056261333as',
            database='ptData'
        )
        cur = conn.cursor()
        cur.execute(sql_album_query, (qobuzId,))
        cur.execute(sql_album_status_query1, (qobuzId,))
        cur.execute(sql_album_status_query2, (qobuzId,))
        conn.commit()
        logger.info(f"albums torrent Insert")
    except Exception as e:
        print(e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()