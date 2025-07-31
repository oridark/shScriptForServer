import io, random, bencodepy, flask, json, time, os, qopy, redApi, ptpimgUploader, configparser, logging, datetime, ggnApi, opsApi, Levenshtein, ptpApi, imdbApi, tmdbApi, bluApi
from flask import request, send_file, abort
from flask_cors import CORS
from threading import Thread, Timer
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from db import deleteOpsStatusAlbumFromMysql, deleteRedStatusAlbumFromMysql, insertSourceTorrentToMysql, getAllBluTorrentsFromMysql, getAlbumsFromMysql, batchUpdateAlbumInfoWithMysql, getAlbumsWithStatusFromMysql, insertAlbumsToMysql, insertAllAlbumsToMysqlFromSqlite, getUncheckedAlbumsFromMysql, insertAlbumsForOpsToMysql, insertAlbumsForRedToMysql, batchUpdatePtpGroupInfoWithMysql, batchInsertAlbumStatusToMysql, batchUpdateAlbumStatusInfoWithMysql, batchUpdateBluTorrentsInfoWithMysql, insertPtpGroupTorrentFileInfoToMysql, insertBluTorrentStatusToMysql, insertSourceSiteTorrentStatusToMysql, batchUpdatePtpGroupInfoWithMysqlInRaw, getPtpGroupsWithTmdb, getBluTorrentsFromMysql, getSourceSiteTorrentsFromMysql, batchUpdatePtpGroupImdbIdWithMysql, insertPtpGroupToMysql, insertBluTorrentToMysql, insertPtpGroupToMysqlInRaw, getPtpGroupsNotInMysql, updatePtpGroupWithMysql, getPtpGroupsFromMysql, getPtpGroupsWithImdb, getPtpGroupsWithoutImdb
from PyMovieDb import IMDB
from pathlib import Path

# 创建线程池
thread_pool = ThreadPoolExecutor(max_workers=10)
imdb = IMDB()
class UploadFailed(Exception):
    def __str__(self):
        msg, *args = self.args
        return msg.format(*args)
class UploadError(Exception):
    """Exception raised for upload errors."""
logger = logging.getLogger(__name__)
# 创建一个服务，把当前这个python文件当做一个服务
server = flask.Flask(__name__)
CORS(server, supports_credentials=True)

CONFIG_FILE = os.path.join("./", "config.ini")

# def asyncz(f):
# 	wraps(f)
# 	def wrapper(*args, **kwargs):
# 		thr = Thread(target=f, args=args, kwargs=kwargs)
# 		thr.start()
# 	return wrapper

# 重新实现asyncz装饰器
def asyncz(f):
    def wrapper(*args, **kwargs):
        return thread_pool.submit(f, *args, **kwargs)
    return wrapper

# 查询albums列表数据
@server.route('/getAlbumsByPramsForPtsite', methods=['get', 'post'])
def getAlbumsByPramsForPtsite():
    ptType = request.values.get('ptType')
    sidePtType = request.values.get('sidePtType')
    orderType = request.values.get('orderType')
    uploadedStatus = request.values.get('uploadedStatus')
    isHiRes = request.values.get('isHiRes')
    downloadStatusFor16bit = request.values.get('downloadStatusFor16bit')
    pageSize = request.values.get('pageSize')
    pageNumber = request.values.get('pageNumber')
    data = getAlbumsWithStatusFromMysql(ptType=ptType,sidePtType=sidePtType,pageSize=pageSize,orderType=orderType,pageNumber=pageNumber,uploadedStatus=uploadedStatus,isHiRes=isHiRes,downloadStatusFor16bit=downloadStatusFor16bit)
    return data

#对查询过未发布在red的album进行重新查询
@server.route('/reCheckUnUploadedAlbumInRed', methods=['get', 'post'])
def reCheckUnUploadedAlbumInRed():
    redApiKey = request.values.get('redApiKey')
    ptType = request.values.get('ptType')
    res = getAlbumsWithStatusFromMysql(ptType='Red', uploadedStatus='2')["list"]
    checkAlbums(res, redApiKey)
    return '批量复查命令已下发'

#对未查询过是否发布在red的album进行查询
@server.route('/checkAlbumsForRed', methods=['get', 'post'])
def checkAlbumInRed():
    redApiKey = request.values.get('redApiKey')
    ptType = request.values.get('ptType')
    res = getUncheckedAlbumsFromMysql(ptType='Red')
    checkAlbums(res, redApiKey)
    return '开始查重,请勿重复点击'

# 批量进行red查重
@asyncz
def checkAlbums(albums, redApiKey):
    if len(albums) > 0:
        print('待查询专辑数量：', len(albums))
        for i,album in enumerate(albums):
            print('总数：', len(albums), '序号：', i, '专辑名：', album['albumTitle'])
            checkToRed(json.loads(album["albumInfo"]), redApiKey)

# 将单个符合条件者去red查询,并将结果写入数据库
def checkToRed(curAlbum, redApiKey):
    time.sleep(2)
    deleteRedStatusAlbumFromMysql(curAlbum["id"])
    blackList = "Ayesha Erotica,Encyclopedia of Jazz,The World's Greatest Jazz Collection,Once Upon a Time in Shaolin,Wu-Tang Clan,The Upholsterers,Your Furniture Was Always Dead,I Was Just Afraid To Tell You,Odds and Sod,Yes Means Nein,Blessed by a Young Death,Tree Full of Secrets,Dawn of the Piper,Under the Covers,Live In Japan Pacific Rim Tour,Strobe Light,Music for Supermarkets,Recycled Records: The Album,Carnival of Light,Trip It Records,Anything by Sip It"
    blackList = blackList.lower()
    if curAlbum["release_date_original"] is None:
        curAlbum["release_date_original"] = '1970-01-01'
    releaseTime = datetime.datetime.strptime(curAlbum["release_date_original"],'%Y-%m-%d')
    limitTime = datetime.datetime.strptime("2010-01-01",'%Y-%m-%d')
    if (curAlbum["genre"]["name"] in "Classical,Classique,Clássica,Christmas,Christmas Music,child,Child,Chamber" or len(curAlbum["title"]) > 60 or curAlbum["title"].lower() in blackList or curAlbum["artist"]["name"].lower() in blackList or limitTime > releaseTime or '&' in curAlbum["title"] or '/' in curAlbum["title"] or '#' in curAlbum["title"] or '/' in curAlbum["artist"]["name"] or 'arious Artist' in curAlbum["artist"]["name"]):
        # 特殊字符等跳过
        # insertToRedAlbum(curAlbum["id"], '0', '1')
        print('跳过：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
    else:
        title = curAlbum["title"]
        if "(" in title:
            title = curAlbum["title"].split('(')[0]
        title = title.replace('’',' ').replace('\'',' ').replace(':',' ')
        time.sleep(2)
        # 查询red数据判断是否已发布
        redSearchResp = searchAlbumInRed(title, redApiKey)
        isUploaded = "2"
        if redSearchResp is None:
            # checkToRed(curAlbum,redApiKey)
            return
        try:
            if (len(redSearchResp['response']["results"]) > 0):
                title = title.replace(" - EP","").replace(" EP","").split("(")[0].split("[")[0].split(" - ")[0].replace(' ','').replace('’','').replace('\'','').replace(':','').lower()
                for resp in redSearchResp['response']["results"]:
                    resp["groupName"] = resp["groupName"].replace(' ','').replace('&rsquo;','').replace(':','').replace('\'','').lower()
                    if resp["groupName"].lower() in title.lower() or title.lower() in resp["groupName"].lower() or title.lower() == resp["groupName"].lower() or checkAlbumsTitleEqual(title.lower(), resp["groupName"].lower()):
                        torrents = resp["torrents"]
                        if (len(torrents) > 0):
                            for torrent in torrents:
                                if(torrent["format"] == "FLAC" and torrent["media"] == "WEB"):
                                    isUploaded = "1"
            if isUploaded == "2":
                # 符合条件，暂计为未发布
                checkByArtistNameResp = checkToRedByArtistName(curAlbum,redApiKey)
                if checkByArtistNameResp["type"] == 2:
                    # insertToRedAlbum(curAlbum["id"], '1', '0')
                    batchInsertAlbumStatusToMysql(data=[(curAlbum["id"], '1')], tableName='albumStatusForPtSiteForRed')
                    print('已发布：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
                else:
                    # insertToRedAlbum(curAlbum["id"], '2', '0')
                    batchInsertAlbumStatusToMysql(data=[(curAlbum["id"], '2')], tableName='albumStatusForPtSiteForRed')
                    print('未发布：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
            else:
                # 不符合条件记录为已发布
                # insertToRedAlbum(curAlbum["id"], '1', '0')
                batchInsertAlbumStatusToMysql(data=[(curAlbum["id"], '1')], tableName='albumStatusForPtSiteForRed')
                print('已发布：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
        except KeyError:
            print('KeyError')

# 将单个符合条件者去red查询,并将结果返回
def checkToRedReturnResp(curAlbum, redApiKey):
    blackList = "Ayesha Erotica,Encyclopedia of Jazz,The World's Greatest Jazz Collection,Once Upon a Time in Shaolin,Wu-Tang Clan,The Upholsterers,Your Furniture Was Always Dead,I Was Just Afraid To Tell You,Odds and Sod,Yes Means Nein,Blessed by a Young Death,Tree Full of Secrets,Dawn of the Piper,Under the Covers,Live In Japan Pacific Rim Tour,Strobe Light,Music for Supermarkets,Recycled Records: The Album,Carnival of Light,Trip It Records,Anything by Sip It"
    blackList = blackList.lower()
    if curAlbum["release_date_original"] is None:
        curAlbum["release_date_original"] = '1970-01-01'
    releaseTime = datetime.datetime.strptime(curAlbum["release_date_original"],'%Y-%m-%d')
    limitTime = datetime.datetime.strptime("2010-01-01",'%Y-%m-%d')
    if (curAlbum["title"].lower() in blackList or len(curAlbum["title"]) > 60 or curAlbum["artist"]["name"].lower() in blackList or limitTime > releaseTime or '&' in curAlbum["title"] or '/' in curAlbum["title"] or '#' in curAlbum["title"] or '/' in curAlbum["artist"]["name"]):
        # 特殊字符等跳过
        return {
            "type": '跳过',
            "redCheckResp": {}
        }
    else:
        title = curAlbum["title"]
        if "(" in title:
            title = curAlbum["title"].split('(')[0]
        title = title.replace('’',' ').replace('\'',' ').replace(':',' ')
        # 查询red数据判断是否已发布
        redSearchResp = searchAlbumInRed(title, redApiKey)
        isUploaded = "0"
        if redSearchResp is None:
            return {
                "type": 500,
                "redCheckResp": redSearchResp,
                "param": title,
            }
        try:
            if (len(redSearchResp['response']["results"]) > 0):
                title = title.replace(" - EP","").replace(" EP","").split("(")[0].split("[")[0].split(" - ")[0].replace(' ','').replace('’','').replace('\'','').replace(':','').lower()
                for resp in redSearchResp['response']["results"]:
                    resp["groupName"] = resp["groupName"].replace(' ','').replace('&rsquo;','').replace(':','').replace('\'','').lower()
                    if resp["groupName"].lower() in title.lower() or title.lower() in resp["groupName"].lower() or title.lower() == resp["groupName"].lower() or checkAlbumsTitleEqual(title.lower(), resp["groupName"].lower()):
                        torrents = resp["torrents"]
                        if (len(torrents) > 0):
                            for torrent in torrents:
                                if(torrent["format"] == "FLAC" and torrent["media"] == "WEB"):
                                    isUploaded = "1"
            if isUploaded == "0":
                checkByArtistNameResp = checkToRedByArtistName(curAlbum, redApiKey)
                deleteRedStatusAlbumFromMysql(curAlbum["id"])
                if checkByArtistNameResp["type"] == '已发':
                    # insertToRedAlbum(curAlbum["id"], '1', '0')
                    batchInsertAlbumStatusToMysql(data=[(curAlbum["id"], '1')], tableName='albumStatusForPtSiteForRed')
                    print('已发布：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
                else:
                    # insertToRedAlbum(curAlbum["id"], '2', '0')
                    batchInsertAlbumStatusToMysql(data=[(curAlbum["id"], '2')], tableName='albumStatusForPtSiteForRed')
                    print('未发布：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
                return {
                    "type": checkByArtistNameResp["type"],
                    "redCheckResp": redSearchResp,
                    "checkByArtistNameResp": checkByArtistNameResp,
                    "param": title,
                }
                # os_system("https://play.qobuz.com/album/"+curAlbum["id"], 7)
            else:
                # 不符合条件记录为已发布
                return {
                    "type": '已发',
                    "redCheckResp": redSearchResp,
                    "param": title,
                }
        except KeyError:
            print('KeyError')
            return {
                "type": 500,
                "redCheckResp": redSearchResp
            }

def checkToRedByArtistName(curAlbum,redApiKey):
    redSearchResp = searchAlbumInRedByArtistName(curAlbum["artist"]["name"],redApiKey)
    type = '未发'
    if redSearchResp is None:
        return {
            "type": 500,
            "redCheckResp": redSearchResp
        }
    try:
        if len(redSearchResp["response"]["torrentgroup"]) > 0:
            for albumGroupOfArtist in redSearchResp["response"]["torrentgroup"]:
                albumGroupOfArtist["groupName"] = albumGroupOfArtist["groupName"].replace(' ','').replace('&rsquo;','').replace(':','').replace('\'','').lower()
                curAlbumTitle = curAlbum["title"].replace(' ','').replace('’','').replace('\'','').replace(':','').lower()
                if curAlbumTitle in albumGroupOfArtist["groupName"] or albumGroupOfArtist["groupName"] in curAlbumTitle or albumGroupOfArtist["groupName"] == curAlbumTitle or checkAlbumsTitleEqual(curAlbumTitle, albumGroupOfArtist["groupName"]):
                    torrents = albumGroupOfArtist["torrent"]
                    if (len(torrents) > 0):
                        for torrent in torrents:
                            if(torrent["format"] == "FLAC" and torrent["media"] == "WEB"):
                                type = '已发'
        return {
            "type": type,
            "redCheckResp": redSearchResp
        }
    except KeyError:
        print('KeyError')
        return {
            "type": 0,
            "redCheckResp": redSearchResp
        }

# 获取某个专辑的查询数据接口
@server.route('/getCheckDataForOps', methods=['get', 'post'])
def getCheckDataForOps():
    qobuzId = request.values.get('qobuzId')
    opsApiKey = request.values.get('opsApiKey')
    res = getAlbumsFromMysql(qobuzId=qobuzId)["list"][0]
    curAlbumQobuzInfo = json.loads(res["albumInfo"])
    checkResult = checkSingleAlbumFromOps(res, opsApiKey)
    return {
        "checkResult": checkResult,
        "qobuzInfo": curAlbumQobuzInfo
    }

# 获取某个专辑的查询数据方法
def checkSingleAlbumFromOps(album, opsApiKey):
    checkResp = checkToOpsReturnResp(json.loads(album["albumInfo"]), opsApiKey)
    return checkResp

#对未查询过是否发布在ops的album进行查询
@server.route('/checkAlbumsForOps', methods=['get', 'post'])
def checkAlbumsForOps():
    opsApiKey = request.values.get('opsApiKey')
    res = getUncheckedAlbumsFromMysql(ptType='Ops')
    checkForOps(res, opsApiKey)
    return '开始查重,请勿重复点击'

#对未发布在ops的album进行复查
@server.route('/reCheckAlbumsForOps', methods=['get', 'post'])
def reCheckAlbumsForOps():
    opsApiKey = request.values.get('opsApiKey')
    res = getAlbumsWithStatusFromMysql(ptType='Ops', uploadedStatus=2)
    print('数量：', res['total'])
    checkForOps(res["list"], opsApiKey)
    return '开始查重,请勿重复点击'

# 批量进行ops查重
@asyncz
def checkForOps(albums, opsApiKey):
    if len(albums) > 0:
        print('待查询专辑数量：', len(albums))
        for i,album in enumerate(albums):
            print('总数：', len(albums), '序号：', i, '专辑名：', album['albumTitle'])
            checkToOps(json.loads(album["albumInfo"]), opsApiKey)

# 将单个符合条件者去ops查询,并将结果写入数据库
def checkToOps(curAlbum, opsApiKey):
    time.sleep(3)
    deleteOpsStatusAlbumFromMysql(curAlbum["id"])
    blackList = "Ayesha Erotica,Encyclopedia of Jazz,The World's Greatest Jazz Collection,Once Upon a Time in Shaolin,Wu-Tang Clan,The Upholsterers,Your Furniture Was Always Dead,I Was Just Afraid To Tell You,Odds and Sod,Yes Means Nein,Blessed by a Young Death,Tree Full of Secrets,Dawn of the Piper,Under the Covers,Live In Japan Pacific Rim Tour,Strobe Light,Music for Supermarkets,Recycled Records: The Album,Carnival of Light,Trip It Records,Anything by Sip It"
    blackList = blackList.lower()
    if curAlbum["release_date_original"] is None:
        curAlbum["release_date_original"] = '1970-01-01'
    releaseTime = datetime.datetime.strptime(curAlbum["release_date_original"],'%Y-%m-%d')
    limitTime = datetime.datetime.strptime("2010-01-01",'%Y-%m-%d')
    if (curAlbum["genre"]["name"] in "Classical,Classique,Clássica,Christmas,Christmas Music,child,Child,Chamber" or len(curAlbum["title"]) > 60 or curAlbum["title"].lower() in blackList or curAlbum["artist"]["name"].lower() in blackList or limitTime > releaseTime or '&' in curAlbum["title"] or '/' in curAlbum["title"] or '#' in curAlbum["title"] or '/' in curAlbum["artist"]["name"] or 'arious Artist' in curAlbum["artist"]["name"]):
        # 特殊字符等跳过
        # insertToOpsAlbum(curAlbum["id"], '0', '1')
        print('跳过：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
    else:
        title = curAlbum["title"]
        if "(" in title:
            title = curAlbum["title"].split('(')[0]
        title = title.replace('’',' ').replace('\'',' ').replace(':',' ')
        # time.sleep(2)
        # 查询red数据判断是否已发布
        opsSearchResp = searchAlbumInOps(title, opsApiKey)
        isUploaded = "2"
        if opsSearchResp is None:
            print(curAlbum["title"])
            time.sleep(3)
            # checkToOps(curAlbum, opsApiKey)
            return
        try:
            if (len(opsSearchResp['response']["results"]) > 0):
                title = title.replace(" - EP","").replace(" EP","").split("(")[0].split("[")[0].split(" - ")[0].replace(' ','').replace('’','').replace('\'','').replace(':','').lower()
                for resp in opsSearchResp['response']["results"]:
                    resp["groupName"] = resp["groupName"].replace(' ','').replace('&rsquo;','').replace(':','').replace('\'','').lower()
                    if resp["groupName"].lower() in title.lower() or title.lower() in resp["groupName"].lower() or title.lower() == resp["groupName"].lower() or checkAlbumsTitleEqual(title.lower(), resp["groupName"].lower()):
                        torrents = resp["torrents"]
                        if (len(torrents) > 0):
                            for torrent in torrents:
                                if(torrent["format"] == "FLAC" and torrent["media"] == "WEB"):
                                    isUploaded = "1"
            if isUploaded == "2":
                time.sleep(2)
                # 符合条件，暂计为未发布
                checkByArtistNameResp = checkToOpsByArtistName(curAlbum, opsApiKey)
                if checkByArtistNameResp["type"] == 2:
                    # insertToOpsAlbum(curAlbum["id"], '1', '0')
                    batchInsertAlbumStatusToMysql(data=[(curAlbum["id"], '1')], tableName='albumStatusForPtSiteForOps')
                    print('已发布：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
                else:
                    # insertToOpsAlbum(curAlbum["id"], '2', '0')
                    batchInsertAlbumStatusToMysql(data=[(curAlbum["id"], '2')], tableName='albumStatusForPtSiteForOps')
                    print('未发布：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
            else:
                # 不符合条件记录为已发布
                # insertToOpsAlbum(curAlbum["id"], '1', '0')
                batchInsertAlbumStatusToMysql(data=[(curAlbum["id"], '1')], tableName='albumStatusForPtSiteForOps')
                print('已发布：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
        except (KeyError,TypeError) as e:
            print('KeyError', '当前专辑：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
            print('KeyError checkToOps', opsSearchResp)

# 将单个符合条件者去red查询,并将结果返回
def checkToOpsReturnResp(curAlbum, opsApiKey):
    blackList = "Ayesha Erotica,Encyclopedia of Jazz,The World's Greatest Jazz Collection,Once Upon a Time in Shaolin,Wu-Tang Clan,The Upholsterers,Your Furniture Was Always Dead,I Was Just Afraid To Tell You,Odds and Sod,Yes Means Nein,Blessed by a Young Death,Tree Full of Secrets,Dawn of the Piper,Under the Covers,Live In Japan Pacific Rim Tour,Strobe Light,Music for Supermarkets,Recycled Records: The Album,Carnival of Light,Trip It Records,Anything by Sip It"
    blackList = blackList.lower()
    if curAlbum["release_date_original"] is None:
        curAlbum["release_date_original"] = '1970-01-01'
    releaseTime = datetime.datetime.strptime(curAlbum["release_date_original"],'%Y-%m-%d')
    limitTime = datetime.datetime.strptime("2010-01-01",'%Y-%m-%d')
    if (curAlbum["genre"]["name"] in "Classical,Classique,Clássica,Christmas,Christmas Music,child,Child,Chamber" or len(curAlbum["title"]) > 60 or curAlbum["title"].lower() in blackList or curAlbum["artist"]["name"].lower() in blackList or limitTime > releaseTime or '&' in curAlbum["title"] or '/' in curAlbum["title"] or '#' in curAlbum["title"] or '/' in curAlbum["artist"]["name"] or 'arious Artist' in curAlbum["artist"]["name"]):
        # 特殊字符等跳过
        # insertToOpsAlbum(curAlbum["id"], '0', '1')
        print('跳过：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
    else:
        title = curAlbum["title"]
        if "(" in title:
            title = curAlbum["title"].split('(')[0]
        title = title.replace('’',' ').replace('\'',' ').replace(':',' ')
        # time.sleep(2)
        # 查询ops数据判断是否已发布
        opsSearchResp = searchAlbumInOps(title, opsApiKey)
        isUploaded = "0"
        if opsSearchResp is None:
            return {
                "type": 500,
                "opsCheckResp": opsSearchResp,
                "param": title,
            }
        try:
            if (len(opsSearchResp['response']["results"]) > 0):
                title = title.replace(" - EP","").replace(" EP","").split("(")[0].split("[")[0].split(" - ")[0].replace(' ','').replace('’','').replace('\'','').replace(':','').lower()
                for resp in opsSearchResp['response']["results"]:
                    resp["groupName"] = resp["groupName"].replace(' ','').replace('&rsquo;','').replace(':','').replace('\'','').lower()
                    if resp["groupName"].lower() in title.lower() or title.lower() in resp["groupName"].lower() or title.lower() == resp["groupName"].lower() or checkAlbumsTitleEqual(title.lower(), resp["groupName"].lower()):
                        torrents = resp["torrents"]
                        if (len(torrents) > 0):
                            for torrent in torrents:
                                if(torrent["format"] == "FLAC" and torrent["media"] == "WEB"):
                                    isUploaded = "1"
            if isUploaded == "0":
                checkByArtistNameResp = checkToOpsByArtistName(curAlbum, opsApiKey)
                deleteOpsStatusAlbumFromMysql(curAlbum["id"])
                if checkByArtistNameResp["type"] == '已发':
                    # insertToOpsAlbum(curAlbum["id"], '1', '0')
                    batchInsertAlbumStatusToMysql(data=[(curAlbum["id"], '1')], tableName='albumStatusForPtSiteForOps')
                    print('已发布：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
                else:
                    # insertToOpsAlbum(curAlbum["id"], '2', '0')
                    batchInsertAlbumStatusToMysql(data=[(curAlbum["id"], '2')], tableName='albumStatusForPtSiteForOps')
                    print('未发布：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
                return {
                    "type": checkByArtistNameResp["type"],
                    "opsCheckResp": opsSearchResp,
                    "checkByArtistNameResp": checkByArtistNameResp,
                    "param": title,
                }
                # os_system("https://play.qobuz.com/album/"+curAlbum["id"], 7)
            else:
                # 不符合条件记录为已发布
                return {
                    "type": '已发',
                    "opsCheckResp": opsSearchResp,
                    "param": title,
                }
        except KeyError:
            print('KeyError checkToOpsReturnResp', '已发布：'+ curAlbum["artist"]["name"] + ' - ' + curAlbum["title"])
            return {
                "type": 500,
                "opsCheckResp": opsSearchResp
            }

def checkToOpsByArtistName(curAlbum, opsApiKey):
    opsSearchResp = searchAlbumInOpsByArtistName(curAlbum["artist"]["name"], opsApiKey)
    type = '未发'
    if opsSearchResp is None:
        return {
            "type": 500,
            "opsCheckResp": opsSearchResp,
            "checkArtistName": curAlbum["artist"]["name"]
        }
    try:
        if len(opsSearchResp["response"]["results"]) > 0:
            for albumGroupOfArtist in opsSearchResp["response"]["results"]:
                albumGroupOfArtist["groupName"] = albumGroupOfArtist["groupName"].replace(' ','').replace('&rsquo;','').replace(':','').replace('\'','').lower()
                curAlbumTtitle = curAlbum["title"].replace(' ','').replace('’','').replace('\'','').replace(':','').lower()
                if curAlbumTtitle in albumGroupOfArtist["groupName"] or albumGroupOfArtist["groupName"] in curAlbumTtitle or albumGroupOfArtist["groupName"] == curAlbumTtitle or checkAlbumsTitleEqual(curAlbumTtitle, albumGroupOfArtist["groupName"]):
                    torrents = albumGroupOfArtist["torrents"]
                    if (len(torrents) > 0):
                        for torrent in torrents:
                            if(torrent["format"] == "FLAC" and torrent["media"] == "WEB"):
                                type = '已发'
        return {
            "type": type,
            "opsCheckResp": opsSearchResp,
            "checkArtistName": curAlbum["artist"]["name"]
        }
    except (KeyError,TypeError) as e:
        print(opsSearchResp)
        print('KeyError checkToOpsByArtistName', curAlbum["artist"]["name"] + ':' + curAlbum["title"])
        return {
            "type": 0,
            "opsCheckResp": opsSearchResp,
            "checkArtistName": curAlbum["artist"]["name"]
        }

#通过编辑距离简易判断相似度
def checkAlbumsTitleEqual(titleQobuz, titleTarget):
    ratioLimit = 0.8
    if len(titleQobuz) > 15 or len(titleTarget) > 15:
        ratioLimit = 0.85
    elif len(titleQobuz) > 30 or len(titleTarget) > 30:
        ratioLimit = 0.9
    elif len(titleQobuz) > 40 or len(titleTarget) > 40:
        ratioLimit = 0.92
    ratio = Levenshtein.ratio(titleQobuz, titleTarget)
    return ratio > ratioLimit

#对未查询过是否发布在ggn的album进行查询
@server.route('/checkAlbumsForGgn', methods=['get', 'post'])
def checkAlbumsForGgn():
    ggnApiKey = request.values.get('ggnApiKey')
    qobuzId = request.values.get('qobuzId')
    if qobuzId != None:
        res = getAlbums(qobuzId=qobuzId)
    else:
        res = getAlbumForPtNoCheck(albumTitle="original game soundtrack", ptWeb="Ggn")
    print('total:', res["total"])
    checkOsts(res["list"], ggnApiKey)
    return '开始查重,请勿重复点击'

#对未发布在ggn的album进行查询
@server.route('/reCheckAlbumsForGgn', methods=['get', 'post'])
def reCheckAlbumsForGgn():
    ggnApiKey = request.values.get('ggnApiKey')
    res = getAlbumForPtChecked(albumTitle="original game soundtrack", ptWeb="Ggn", uploadedStatus="2")
    print('total:', res["total"])
    checkOsts(res["list"], ggnApiKey)
    return '开始复查,请勿重复点击'

# 批量进行ggn查重
@asyncz
def checkOsts(osts, ggnApiKey):
    if len(osts) > 0:
        print('待查专辑数量：', len(osts))
        for ost in osts:
            time.sleep(3)
            ostInfo = json.loads(ost["albumInfo"])
            if ('/' in ostInfo["title"] or '#' in ostInfo["title"] or '/' in ostInfo["artist"]["name"]):
                deletePtAlbum(ptWeb='ggn',qobuzId=ostInfo["id"])
                # insertToPtAlbum(ptWeb='ggn',qobuzId=ostInfo["id"],uploadedStatus='0',isSkip='1')
            else:
                checkToGgn(json.loads(ost["albumInfo"]), ggnApiKey)

def checkToGgn(ostInfo, ggnApiKey):
    res = searchAlbumInGgn(ostInfo["title"], ggnApiKey)
    isUploaded = '2'
    if res != None and type(res["response"]) != list:
        for groupKey in res["response"]:
            group = res["response"][groupKey]
            if type(group["Torrents"]) != list:
                for torrentKey in group["Torrents"]:
                    torrent = group["Torrents"][torrentKey]
                    if torrent["Format"] == "FLAC":
                        isUploaded = '1'
    deletePtAlbum(ptWeb='ggn',qobuzId=ostInfo["id"])
    if isUploaded == '2':
        print(ostInfo["title"] + '未发布')
    else:
        print(ostInfo["title"] + '已发布')          
    # insertToPtAlbum(ptWeb='ggn',qobuzId=ostInfo["id"],uploadedStatus=isUploaded)
    batchInsertAlbumStatusToMysql(data=[(ostInfo["id"], '1')], tableName=('albumStatusForPtSiteFor'+ptType))

# 查询albums列表数据
@server.route('/getAlbumsByPrams', methods=['get', 'post'])
def getAlbumsByPrams():
    # ptType = request.values.get('ptType')
    orderType = request.values.get('orderType')
    qobuzId = request.values.get('qobuzId')
    albumTitle = request.values.get('albumTitle')
    isHiRes = request.values.get('isHiRes')
    downloadStatusFor16bit = request.values.get('downloadStatusFor16bit')
    downloadStatusFor24bit = request.values.get('downloadStatusFor24bit')
    pageSize = request.values.get('pageSize')
    pageNumber = request.values.get('pageNumber')
    # data = getAlbums(ptType=ptType,pageSize=pageSize,orderType=orderType,pageNumber=pageNumber,qobuzId=qobuzId,albumTitle=albumTitle,isHiRes=isHiRes,downloadStatusFor16bit=downloadStatusFor16bit,downloadStatusFor24bit=downloadStatusFor24bit)
    data = getAlbumsFromMysql(pageSize=pageSize,orderType=orderType,pageNumber=pageNumber,qobuzId=qobuzId,albumTitle=albumTitle,isHiRes=isHiRes,downloadStatusFor16bit=downloadStatusFor16bit,downloadStatusFor24bit=downloadStatusFor24bit)
    return data

# 获取单个red种子状态
@server.route('/getRedUploadedAlbum', methods=['get', 'post'])
def getRedUploadedAlbum():
    title = request.values.get('title')
    data = getAlbumsFromMysql(albumTitle=title)
    if data['total'] == 1:
        return data['list'][0]
    else:
        return {}

# 单个专辑强制下载接口
@server.route('/reDownloadCommond', methods=['get', 'post'])
def reDownloadCommond():
    ptpImgApiKey = request.values.get('ptpImgApiKey')
    if ptpImgApiKey == None or ptpImgApiKey == '':
        ptpImgApiKey = 'e3cef9d4-8388-4fb5-8034-6065f32414cc'
    setConfig('ptpImgApiKey', ptpImgApiKey)
    qobuzId = request.values.get('qobuzId')
    # res = getAlbums(qobuzId=qobuzId)["list"][0]
    res = getAlbumsFromMysql(qobuzId=qobuzId)["list"][0]
    reDownloadAlbum(res)
    return '强制下载任务命令已下发'

# 单个专辑强制下载方法
@asyncz
def reDownloadAlbum(album):
    if album['fileDirFor24bit'] != None and album['fileDirFor24bit'] != '':
        deleteAlbumFile(album['fileDirFor24bit'])
    if album['fileDirFor16bit'] != None and album['fileDirFor16bit'] != '':
        deleteAlbumFile(album['fileDirFor16bit'])
    # updateAlbum(qobuzId=album['qobuzId'],downloadStatusFor16bit='3')
    batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': album['qobuzId'], 'downloadStatusFor16bit': '3'}])
    # os.system(f"qobuz-dl -p")
    # time.sleep(1)
    # os.system(f"qobuz-dl dl https://play.qobuz.com/album/\"{album['qobuzId']}\" -q6")
    # res = getAlbums(qobuzId=album['qobuzId'])["list"][0]
    # url = smms_upload(res['fileDirFor16bit'] + '/spectrogram.png', key)
    # updateAlbum(qobuzId=album['qobuzId'],spectrogramUrlFor16bit=url)
    if album["isHiRes"] == "1":
        # updateAlbum(qobuzId=album['qobuzId'],downloadStatusFor24bit='3')
        batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': album['qobuzId'], 'downloadStatusFor24bit': '3'}])
        # os.system(f"qobuz-dl -p")
        # time.sleep(1)
        # os.system(f"qobuz-dl dl https://play.qobuz.com/album/\"{album['qobuzId']}\" -q27")
        # res = getAlbums(qobuzId=album['qobuzId'])["list"][0]
        # url = smms_upload(res['fileDirFor24bit'] + '/spectrogram.png', key)
        # updateAlbum(qobuzId=album['qobuzId'],spectrogramUrlFor24bit=url)

# 批量下载接口
@server.route('/downloadBatch', methods=['get', 'post'])
def downloadBatch():
    ptType = request.values.get('ptType')
    sidePtType = request.values.get('sidePtType')
    ptpImgApiKey = request.values.get('ptpImgApiKey')
    orderType = request.values.get('orderType')
    isHiRes = request.values.get('isHiRes')
    if ptpImgApiKey == None or ptpImgApiKey == '':
        ptpImgApiKey = 'e3cef9d4-8388-4fb5-8034-6065f32414cc'
    setConfig('ptpImgApiKey', ptpImgApiKey)
    downloadNumber = request.values.get('downloadNumber')
    # res = getAlbumsForPtSite(ptType=ptType,sidePtType=sidePtType,orderType=orderType, isHiRes=isHiRes, uploadedStatus='2', pageSize=downloadNumber, downloadStatusFor16bit='0',downloadStatusFor24bit='0')["list"]
    res = getAlbumsWithStatusFromMysql(ptType=ptType,sidePtType=sidePtType,orderType=orderType, isHiRes=isHiRes, uploadedStatus='2', pageSize=downloadNumber, downloadStatusFor16bit='0',downloadStatusFor24bit='0')["list"]
    reDownloadAlbums(res)
    return '任务已下发'

# 批量下载方法
@asyncz
def reDownloadAlbums(albums):
    for album in albums:
        if album['fileDirFor24bit'] != None and album['fileDirFor24bit'] != '':
            deleteAlbumFile(album['fileDirFor24bit'])
        if album['fileDirFor16bit'] != None  and album['fileDirFor16bit'] != '':
            deleteAlbumFile(album['fileDirFor16bit'])
        # updateAlbum(qobuzId=album['qobuzId'],downloadStatusFor16bit='3')
        batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': album['qobuzId'], 'downloadStatusFor16bit': '3'}])
        # os.system(f"qobuz-dl -p")
        # time.sleep(1)
        # os.system(f"qobuz-dl dl https://play.qobuz.com/album/\"{album['qobuzId']}\" -q6")
        # res = getAlbums(qobuzId=album['qobuzId'])["list"][0]
        # url = smms_upload(res['fileDirFor16bit'] + '/spectrogram.png', key)
        # updateAlbum(qobuzId=album['qobuzId'],spectrogramUrlFor16bit=url)
        if album["isHiRes"] == "1":
            # updateAlbum(qobuzId=album['qobuzId'],downloadStatusFor24bit='3')
            batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': album['qobuzId'], 'downloadStatusFor24bit': '3'}])
            # os.system(f"qobuz-dl -p")
            # time.sleep(1)
            # os.system(f"qobuz-dl dl https://play.qobuz.com/album/\"{album['qobuzId']}\" -q27")
            # res = getAlbums(qobuzId=album['qobuzId'])["list"][0]
            # url = smms_upload(res['fileDirFor24bit'] + '/spectrogram.png', key)
            # updateAlbum(qobuzId=album['qobuzId'],spectrogramUrlFor24bit=url)

# 手动更新某个专辑的发布状态
@server.route('/updateAlbumStatus', methods=['get', 'post'])
def updateAlbumStatus():
    qobuzId = request.values.get('qobuzId')
    ptType = request.values.get('ptType')
    user = request.values.get('user')
    # if ptType == 'red':
    #     updateRedAlbums(qobuzId=qobuzId,uploadedStatus='1')
    # else:
    res = getAlbumsWithStatusFromMysql(qobuzId=qobuzId, ptType=ptType)
    if len(res['list']) == 0:
        # insertToPtAlbum(ptWeb=ptType,qobuzId=qobuzId,uploadedStatus='1')
        batchInsertAlbumStatusToMysql(data=[(qobuzId, '1')], tableName=('albumStatusForPtSiteFor'+ptType))
        print('已添加发布')
    else:
        # updatePtAlbums(ptWeb=ptType,qobuzId=qobuzId,uploadedStatus='1')
        batchUpdateAlbumStatusInfoWithMysql(updates=[{'qobuzId': qobuzId, 'uploadedStatus': '1'}], tableName=('albumStatusForPtSiteFor'+ptType))
        print('已修改为发布')
    return 'update命令已下发'

# 获取某个专辑的red查询数据接口
@server.route('/getCheckData', methods=['get', 'post'])
def getCheckData():
    qobuzId = request.values.get('qobuzId')
    redApiKey = request.values.get('redApiKey')
    res = getAlbumsFromMysql(qobuzId=qobuzId)["list"][0]
    curAlbumQobuzInfo = json.loads(res["albumInfo"])
    checkResult = checkSingleAlbumFromRed(res, redApiKey)
    return {
        "checkResult": checkResult,
        "qobuzInfo": curAlbumQobuzInfo
    }

# 获取某个专辑的查询数据方法
def checkSingleAlbumFromRed(album, redApiKey):
    checkResp = checkToRedReturnResp(json.loads(album["albumInfo"]), redApiKey)
    return checkResp

#对单个album进行red查重
@server.route('/reCheckAlbum', methods=['get', 'post'])
def reCheckAlbum():
    ptType = request.values.get('ptType')
    qobuzId = request.values.get('qobuzId')
    redApiKey = request.values.get('redApiKey')
    album = getAlbumsFromMysql(qobuzId=qobuzId)['list'][0]
    checkToRed(json.loads(album["albumInfo"]), redApiKey)
    return '复查命令已下发,请勿重复点击'

# 上传qobuz专辑id列表并获取数据
@server.route('/uploadQobuzAlbums', methods=['get', 'post'])
def uploadQobuzAlbums():
    data = request.values.get('data')
    if data != None:
        data = json.loads(data)
    else:
        data = []
    getOstAlbumsFromUploadList(0, data)
    print("开始OST数据获取, 总量为", len(data))
    return '拉取qobuz数据命令已下发'

# 查询游戏ost专辑
@asyncz
def getOstAlbumsFromUploadList(index, data):
    print("开始check：", index, ', title:', data[index]["title"])
    if data[index]['qobuzId'] !=None or data[index]['qobuzId'] != '':
        album = getAlbumsFromMysql(qobuzId=data[index]['qobuzId'])
        if album["total"] == 0:
            print("库内无记录")
            albumDetail = getAlbumInfoFromQobuz(data[index]['qobuzId'])
            if albumDetail != None:
                print("qobuz可获取到数据")
                isHiRes = '0'
                if albumDetail["hires"]:
                    isHiRes = '1'
                print("开始新增OST数据：", albumDetail["title"], ', qobuzId:', data[index]['qobuzId'])
                insertAlbum(data[index]['qobuzId'],json.dumps(albumDetail),albumDetail["title"],albumDetail["release_date_original"],isHiRes,albumDetail["tracks_count"])
    else:
        print('无qobuzId')
    if len(data) > index + 1:
        getOstAlbumsFromUploadList(index + 1, data)

# 获取qobuz数据接口
@server.route('/getQobuzAlbumsInfo', methods=['get', 'post'])
def getQobuzAlbumsInfo():
    checkType = request.values.get('checkType')
    if checkType == "new release":
        getAlbumsInfoFromQobuzNewRelease()
    elif checkType == "OST":
        getOstAlbums()
    else:
        getAlbumsFromQobuzPlaylist(50, 0)
    return '拉取qobuz数据命令已下发'

# 批量查询qobuz playlist列表并根据返回写入albums库
@asyncz
def getAlbumsFromQobuzPlaylist(limit=50,offset=0):
    client = initialize_client()
    playlist = client.get_qobuz_playlist(limit=limit, offset=offset)
    print("开始本轮playlist数据获取, 总量为",playlist["playlists"]["total"])
    print(f"limit为{limit},offset为{offset}")
    try:
        if len(playlist["playlists"]["items"]) > 0:
            for i,playlistItem in enumerate(playlist["playlists"]["items"]):
                checkAlbumsList(playlistItem['id'])
                print('offset为', offset + i)
            getAlbumsFromQobuzPlaylist(limit,offset=offset+50)
        else:
            print("该类别下专辑信息已全部下载完成")
    except NameError:
        time.sleep(5)
        getAlbumsFromQobuzPlaylist(limit,offset)

def checkAlbumsList(playlistId):
    client = initialize_client()
    playlistAlbums = client.get_plist_albums(playlistId)
    print('开始下一个playlist数据获取, 总量为',playlistAlbums["tracks"]["total"])
    try:
        if len(playlistAlbums["tracks"]["items"]) > 0:
            for i,album in enumerate(playlistAlbums["tracks"]["items"]):
                playlistAlbums["tracks"]["items"][i] = album["album"]
            insertAlbumIntoAlbums(playlistAlbums["tracks"]["items"])
            time.sleep(2)
    except Exception as e:
        time.sleep(2)
        print(e)
        print('当前playlist获取失败')

# 获取专辑详情信息
def getAlbumInfoFromQobuz(albumId):
    client = initialize_client()
    albumInfo = client.get_album_meta(albumId)
    return albumInfo

# 批量查询qobuz new release列表并写入albums库
@asyncz
def getAlbumsInfoFromQobuzNewRelease(limit=50,offset=0):
    qobuzAlbums = {}
    client = initialize_client()
    qobuzAlbums = client.get_album_list(limit=limit, offset=offset)
    print("开始本轮new release数据获取, 总量为",qobuzAlbums["albums"]["total"], ' 当前offset:', offset)
    try:
        if len(qobuzAlbums["albums"]["items"]) > 0:
            insertAlbumIntoAlbums(qobuzAlbums["albums"]["items"])
            time.sleep(1)
            getAlbumsInfoFromQobuzNewRelease(limit,offset=offset+50)
        else:
            print("该类别下专辑已全部获取")
            os.system(f"qobuz-dl -p")
    except Exception as e:
        time.sleep(2)
        print(e)
        getAlbumsInfoFromQobuzNewRelease(limit,offset=offset+50)

# 将qobuz列表挨个查询本地库，未在库中则写入库
def insertAlbumIntoAlbums(albums):
    for albumInQobuz in albums:
        todayDate = time.strftime("%Y-%m-%d", time.localtime())
        todayDate = datetime.datetime.strptime(todayDate,'%Y-%m-%d')
        if albumInQobuz["release_date_original"] is None:
            albumInQobuz["release_date_original"] = '1970-01-01'
        try:
            releaseTime = datetime.datetime.strptime(albumInQobuz["release_date_original"],'%Y-%m-%d')
            limitTime = datetime.datetime.strptime("2010-01-01",'%Y-%m-%d')
        except ValueError:
            print(ValueError)
        blackList = "Ayesha Erotica,Encyclopedia of Jazz,The World's Greatest Jazz Collection,Once Upon a Time in Shaolin,Wu-Tang Clan,The Upholsterers,Your Furniture Was Always Dead,I Was Just Afraid To Tell You,Odds and Sod,Yes Means Nein,Blessed by a Young Death,Tree Full of Secrets,Dawn of the Piper,Under the Covers,Live In Japan Pacific Rim Tour,Strobe Light,Music for Supermarkets,Recycled Records: The Album,Carnival of Light,Trip It Records,Anything by Sip It"
        blackList = blackList.lower()
        if (albumInQobuz["genre"]["name"] in "Classical,Classique,Clássica,Christmas,Christmas Music,child,Child,Chamber"or len(albumInQobuz["title"]) > 60 or albumInQobuz["title"].lower() in blackList or albumInQobuz["artist"]["name"].lower() in blackList or limitTime > releaseTime or '&' in albumInQobuz["title"] or '/' in albumInQobuz["title"] or '#' in albumInQobuz["title"] or '/' in albumInQobuz["artist"]["name"] or 'arious Artist' in albumInQobuz["artist"]["name"]):
            # 特殊字符等跳过
            print('insert Qobuz 跳过：', albumInQobuz["artist"]["name"], ' name: ', albumInQobuz["title"])
        else:
            if ('Original Game Soundtrack' in albumInQobuz["title"]):
                limitTime = datetime.datetime.strptime("2010-01-01",'%Y-%m-%d')
            if limitTime < releaseTime and todayDate > releaseTime:
                time.sleep(1)
                album = getAlbumsFromMysql(qobuzId=albumInQobuz["id"])
                if album["total"] == 0:
                    albumDetail = getAlbumInfoFromQobuz(albumInQobuz["id"])
                    if albumDetail != None:
                        isHiRes = '0'
                        if albumInQobuz["hires"]:
                            isHiRes = '1'
                        print('写入新的专辑信息：',albumInQobuz["artist"]["name"],' - ',albumInQobuz["title"], albumInQobuz["release_date_original"])
                        # insertAlbum(albumInQobuz["id"],json.dumps(albumDetail),albumInQobuz["title"],albumInQobuz["release_date_original"],isHiRes,albumDetail["tracks_count"])
                        # qobuzId,albumInfo,albumTitle,releaseTime,isHiRes,trackNumber
                        insertAlbumsToMysql([(
                            albumInQobuz["id"],json.dumps(albumDetail),albumInQobuz["title"],albumInQobuz["release_date_original"],isHiRes,'0','0','0',albumDetail["tracks_count"]
                        )])

# 查询游戏ost专辑
@asyncz
def getOstAlbums(limit=50,offset=0):
    client = initialize_client()
    ostAlbums = client.search_albums(query="original game soundtrack",offset=offset, limit=limit)
    try:
        print("开始本轮OST数据获取, 总量为",ostAlbums["albums"]["total"])
        print(f"limit为{limit},offset为{offset}")
        if ostAlbums != None and len(ostAlbums["albums"]["items"]) > 0:
            insertAlbumIntoAlbums(ostAlbums["albums"]["items"])
            time.sleep(1)
            getOstAlbums(limit,offset=offset+50)
        else:
            print("该类别下专辑已全部获取")
            os.system(f"qobuz-dl -p")
    except NameError:
        time.sleep(1)
        # getOstAlbums(limit,offset=offset+50)

#上传图片到ptpimg
@server.route('/imgUpload', methods=['get', 'post'])
def imgUpload():
    type = request.values.get('type')
    ptpImgKey = request.values.get('ptpImgKey')
    image = request.values.get('image')
    # ptpImgKey = "e3cef9d4-8388-4fb5-8034-6065f32414cc"
    return ptpimg_upload(image, type, ptpImgKey)

def ptpimg_upload(image, type, ptpImgKey):
    if (ptpImgKey == None or ptpImgKey == ''):
        return ''
    uploader = ptpimgUploader.PtpimgUploaderClient(ptpImgKey, None)
    if type == 'file':
        return uploader.upload_file(image)
    else:
        return uploader.upload_url(image)

# 查获取qobuz客户端
def initialize_client():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    email = config["DEFAULT"]["email"]
    password = config["DEFAULT"]["password"]
    app_id = config["DEFAULT"]["app_id"]
    secrets = [
        secret for secret in config["DEFAULT"]["secrets"].split(",") if secret
    ]
    return qopy.Client(email, password, app_id, secrets)

# 查询专辑在red上的数据
def searchAlbumInGgn(title, ggnApiKey):
    title = title.split(" (")[0]
    return ggnApi.ggnApiClient(ggnApiKey).searchSource(groupname = title)

# 查询专辑在red上的数据
def searchAlbumInRed(title, redApiKey):
    title = title.replace(" - EP","").replace(" EP","").split("(")[0].split("[")[0].split(" - ")[0]
    # userInfo = getUserAccountInfo("red")
    return redApi.redApiClient(redApiKey).searchSource(searchstr = title)

# 查询专辑在ops上的数据
def searchAlbumInOps(title, opsApiKey):
    title = title.replace(" - EP","").replace(" EP","").split("(")[0].split("[")[0].split(" - ")[0]
    # userInfo = getUserAccountInfo("red")
    return opsApi.opsApiClient(opsApiKey).searchSource(searchstr = title)

# 查询red歌手名对应专辑
def searchAlbumInRedByArtistName(artistName, redApiKey):
    return redApi.redApiClient(redApiKey).searchSourceByArtist(artistName = artistName)

# 查询ops歌手名对应专辑
def searchAlbumInOpsByArtistName(artistName, opsApiKey):
    return opsApi.opsApiClient(opsApiKey).searchSourceByArtist(artistName = artistName)

# 查询在ptp上的group数据
def searchGroupInPtp(page, apikey, apiUser, isPopcorn = False):
    if isPopcorn == False:
        return ptpApi.ptpApiClient(apikey, apiUser).getPtpGroups(page)
    else:
        return ptpApi.ptpApiClient(apikey, apiUser).getPtpGroupsWithGoldenPopcorn(page)

# 查询在ptp上的movie数据
def searchGroupInfoInPtpById(id, apikey, apiUser):
    return ptpApi.ptpApiClient(apikey, apiUser).getPtpGroupInfoById(id)

# 拉取在blu上的movie数据
def getTorrentListInfoInBlu(tmdbId):
    # apikey = 'ImS23Pvc3IH7Lp9QzdFvLmeE9QGTNPFstdS2m0IvcEuUFGQliQ8o2ZeVck5zrITM1OWHpGAyt9g6nYojFpc6c1TyqpXFkqi22Mfr' # blu-oridark：apikey
    # apikey = 'xrD434nj1Xl54FVY0PB07WNK9dr987rIrTAb9mQudXV4zhJbqdGCoM5AyQhGC57EFbQ8B3dFWkVlAzpoAoFkRuCUuyDGLq85myJl' # blu-ohmyjackyrrr：apikey
    # apikey = 'hrSJDMqKKHvRuI85Dsx2ZBD0SzmE8E6SAeWLH1AxBXA4kyqcx8uLexsiKNTr4e30mv7F4Qda3g8JgxPm8rcQE85J8wmvmxhflE4t' # blu-qchakula：apikey
    # apikey = '4pC9cH5DgzMkHj70d8dsIPnyYWyGx4KbLYKQkb5vDnq7BmGR7N3jhHEbR1FpviV4Wq71LOe75KSxsLUwOiL6vBlGK1YLCoqwNXuj' # blu-biukelan：apikey
    apikey = 'J2mfyg9vujX0t9eMZ3NeFKzy01vHArMS90vP1Ns3JhvIQyN0xHv39I1vi8TVANdMdycB3EpIUCvcc9viur5WN85X4mWqhNbzQO2q' # blu-lataviastal：apikey
    return bluApi.bluApiClient(apikey).filterBluTorrent(tmdbId = tmdbId)

# 拉取在blu上的torrent种子文件
def getTorrentFileInBlu(torrentId):
    # apikey = 'ImS23Pvc3IH7Lp9QzdFvLmeE9QGTNPFstdS2m0IvcEuUFGQliQ8o2ZeVck5zrITM1OWHpGAyt9g6nYojFpc6c1TyqpXFkqi22Mfr' # blu-oridark：apikey
    # apikey = 'xrD434nj1Xl54FVY0PB07WNK9dr987rIrTAb9mQudXV4zhJbqdGCoM5AyQhGC57EFbQ8B3dFWkVlAzpoAoFkRuCUuyDGLq85myJl' # blu-ohmyjackyrrr：apikey
    # apikey = 'hrSJDMqKKHvRuI85Dsx2ZBD0SzmE8E6SAeWLH1AxBXA4kyqcx8uLexsiKNTr4e30mv7F4Qda3g8JgxPm8rcQE85J8wmvmxhflE4t' # blu-qchakula：apikey
    # apikey = '4pC9cH5DgzMkHj70d8dsIPnyYWyGx4KbLYKQkb5vDnq7BmGR7N3jhHEbR1FpviV4Wq71LOe75KSxsLUwOiL6vBlGK1YLCoqwNXuj' # blu-biukelan：apikey
    apikey = 'J2mfyg9vujX0t9eMZ3NeFKzy01vHArMS90vP1Ns3JhvIQyN0xHv39I1vi8TVANdMdycB3EpIUCvcc9viur5WN85X4mWqhNbzQO2q' # blu-lataviastal：apikey
    return bluApi.bluApiClient(apikey).getBluTorrentFile(torrentId = torrentId)

# 上传发布种子
def uploadTorrentToBlu(params):
    apikey = 'ImS23Pvc3IH7Lp9QzdFvLmeE9QGTNPFstdS2m0IvcEuUFGQliQ8o2ZeVck5zrITM1OWHpGAyt9g6nYojFpc6c1TyqpXFkqi22Mfr'
    return bluApi.bluApiClient(apikey).uploadBluTorrent(params)

# 在ptp上下载种子
def downloadTorrentFileInPtpById(id, dest, apikey, apiUser):
    return ptpApi.ptpApiClient(apikey, apiUser).getPtpTorrentFileById(id, dest)

# 在imdb上获取信息
def getInfoFroImdbById(id):
    return imdbApi.imdbApiClient().getImdbById(id)

# 在imdb上获取信息
def getTmdbInfoFroImdbById(id):
    apikey = '394403946ff4fbf535ebe64eee01c669'
    return tmdbApi.tmdbApiClient(apikey).getTmdbByImdbid(id)


# 调用系统shell命令执行qobuz下载
@asyncz
def os_system(url, quality):
    os.system(f"qobuz-dl dl \"{url}\" -q\"{quality}\"")
    return ''

# 检测是否有正在下载的任务，若无则触发新一轮的队列下载,若有则跳过，若有且超时则删除错误下载资源并重置下载状态
def checkUndownloadedAlbums():
    # downloadStatus 0:未下载，1：已下载，2：下载失败，3：队列中，4：下载中
    albumListFor16Bit = getAlbumsFromMysql(downloadStatusFor16bit="4")["list"]
    albumListFor24Bit = getAlbumsFromMysql(downloadStatusFor24bit="4")["list"]
    albumListForMp3320 = getAlbumsFromMysql(downloadStatusFor24bit="4")["list"]
    # print('albumListFor16Bit', len(albumListFor16Bit))
    # print('albumListFor24Bit', len(albumListFor24Bit))
    # print('albumListForMp3320', len(albumListForMp3320))
    if len(albumListFor16Bit) > 0 and (time.time() - albumListFor16Bit[0]['downloadStartTime']) > 60:
        # updateAlbum(qobuzId=albumListFor16Bit[0]['qobuzId'],downloadStatusFor16bit='3')
        batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': albumListFor16Bit[0]['qobuzId'], 'downloadStatusFor16bit': '3'}])
        if albumListFor16Bit[0]['fileDirFor16bit'] != None and albumListFor16Bit[0]['fileDirFor16bit'] != '':
            deleteAlbumFile(albumListFor16Bit[0]['fileDirFor16bit'])
    elif len(albumListFor24Bit) > 0 and (time.time() - albumListFor24Bit[0]['downloadStartTime']) > 60:
        # updateAlbum(qobuzId=albumListFor24Bit[0]['qobuzId'],downloadStatusFor24bit='3')
        batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': albumListFor24Bit[0]['qobuzId'], 'downloadStatusFor24bit': '3'}])
        if albumListFor24Bit[0]['fileDirFor24bit'] != None and albumListFor24Bit[0]['fileDirFor24bit'] != '':
            deleteAlbumFile(albumListFor24Bit[0]['fileDirFor24bit'])
    elif len(albumListForMp3320) > 0 and (time.time() - albumListForMp3320[0]['downloadStartTime']) > 60:
        # updateAlbum(qobuzId=albumListForMp3320[0]['qobuzId'],downloadStatusForMp3320='3')
        batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': albumListForMp3320[0]['qobuzId'], 'downloadStatusForMp3320': '3'}])
        if albumListForMp3320[0]['fileDirForMp3320'] != None and albumListForMp3320[0]['fileDirForMp3320'] != '':
            deleteAlbumFile(albumListForMp3320[0]['fileDirForMp3320'])
    elif len(albumListFor16Bit) > 0:
        print(albumListFor16Bit[0]['albumTitle'] + ' 16Bit下载中')
    elif len(albumListFor24Bit) > 0:
        print(albumListFor24Bit[0]['albumTitle'] + ' 24Bit下载中')
    elif len(albumListForMp3320) > 0:
        print(albumListForMp3320[0]['albumTitle'] + ' Mp3 320下载中')
    elif len(albumListFor16Bit) == 0 and len(albumListFor24Bit) == 0:
        downloadQobuzAlbum()

# 下载队列资源
def downloadQobuzAlbum():
    # 查询red未发布未下载专辑
    albums = getAlbumsFromMysql(pageSize=1,downloadStatusFor16bit="3")
    albumListFor16Bit = albums["list"]
    if len(albumListFor16Bit) > 0:
        print('队列专辑剩余数量：', albums["total"])
    if len(albumListFor16Bit) > 0:
        os.system(f"qobuz-dl -p")
        time.sleep(1)
        curAlbum = json.loads(albumListFor16Bit[0]["albumInfo"])
        # updateAlbum(qobuzId=curAlbum['id'],downloadStatusFor16bit='4',downloadStartTime=time.time())
        batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': curAlbum['id'], 'downloadStatusFor16bit': '4', 'downloadStartTime': time.time()}])
        os.system(f"qobuz-dl dl https://play.qobuz.com/album/\"{curAlbum['id']}\" -q6")
        res = getAlbumsFromMysql(qobuzId=curAlbum['id'])["list"]
        if len(res) > 0:
            try:
                ptpimg_upload_backend(json.loads(res[0]['albumInfo'])['image']['large'], curAlbum['id'], 0)
            except Exception as e:
                print(e)
            if res[0]['fileDirFor16bit'] != None or res[0]['fileDirFor16bit'] != '':
                print(res[0]['fileDirFor24bit'])
                try:
                    ptpimg_upload_backend(res[0]['fileDirFor16bit'] + '_spectrogram.png', curAlbum['id'], 16)
                except Exception as e:
                    print(e)
        if albumListFor16Bit[0]["isHiRes"] == "1" or albumListFor16Bit[0]["isHiRes"] == 1:
            os.system(f"qobuz-dl -p")
            time.sleep(1)
            # 更新为下载中并记录时间，执行下载
            # updateAlbum(qobuzId=curAlbum['id'],downloadStatusFor24bit='4',downloadStartTime=time.time())
            batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': curAlbum['id'], 'downloadStatusFor24bit': '4', 'downloadStartTime': time.time()}])
            os.system(f"qobuz-dl dl https://play.qobuz.com/album/\"{curAlbum['id']}\" -q27")
            res = getAlbumsFromMysql(qobuzId=curAlbum['id'])["list"]
            if len(res) > 0:
                if res[0]['fileDirFor24bit'] != None or res[0]['fileDirFor24bit'] != '':
                    print(res[0]['fileDirFor24bit'])
                    try:
                        ptpimg_upload_backend(res[0]['fileDirFor24bit'] + '_spectrogram.png', curAlbum['id'],24)
                    except Exception as e:
                        print(e)
        os.system(f"qobuz-dl -p")
        time.sleep(1)
        # 更新为下载中并记录时间，执行下载
        # updateAlbum(qobuzId=curAlbum['id'],downloadStatusForMp3320='4',downloadStartTime=time.time())
        batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': curAlbum['id'], 'downloadStatusForMp3320': '4', 'downloadStartTime': time.time()}])
        os.system(f"qobuz-dl dl https://play.qobuz.com/album/\"{curAlbum['id']}\" -q5")
        os.system(f"/home/server/batch_convert.sh /home/download /home/server/convert_music.sh")

@asyncz
def ptpimg_upload_backend(image, qobuzId, type):
    uploader = ptpimgUploader.PtpimgUploaderClient(getConfig("ptpImgApiKey"), None)
    if type == 0:
        resp = uploader.upload_url(image)
        # updateAlbum(qobuzId=qobuzId,coverUrlTranslate=resp[0])
        batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': qobuzId, 'coverUrlTranslate': resp[0]}])
    elif type == 16:
        resp = uploader.upload_file(image)
        # updateAlbum(qobuzId=qobuzId,spectrogramUrlFor16bit=resp[0])
        batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': qobuzId, 'spectrogramUrlFor16bit': resp[0]}])
    elif type == 24:
        resp = uploader.upload_file(image)
        # updateAlbum(qobuzId=qobuzId,spectrogramUrlFor24bit=resp[0])
        batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': qobuzId, 'spectrogramUrlFor24bit': resp[0]}])

#删除某个已下载的音乐资源
def deleteAlbumFile(fileDir):
    if fileDir != None and fileDir !='':
        os.system(f"rm -rf \"{fileDir}\"*")
    else:
        print('fileDir: ' + fileDir)
    print('已删除完成')

# 向配置文件中添加属性
def setConfig(attr, value):
    config = configparser.ConfigParser()
    config.read('config.ini')
    config.set('DEFAULT', attr, value)
    with open(CONFIG_FILE, "w") as configfile:
        config.write(configfile)

# 读取配置文件中的属性值
def getConfig(attr):
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config.get('DEFAULT', attr)

#获取ptp group
@server.route('/getPtpGroups', methods=['get', 'post'])
def getPtpGroups():
    page = request.values.get('page')
    triggerTime=int(time.time())
    isPopcorn = False if request.values.get('isPopcorn') == 1 else False
    fetchPtpGroups(page=page, triggerTime=triggerTime, isPopcorn = isPopcorn)

# 获取ptp group数据并进行处理, 间隔40-60秒
# @asyncz
def fetchPtpGroups(page="0", triggerTime=int(time.time()), isPopcorn = False):
    # page = '1'
    apikey = 'PfCNmsaxquORb5uKEq4BKOHdcczHPnuv'
    apiUser = 'e7vUjD0Rd0yF5iUh'
    thisFetchTimeStart = int(time.time())
    try:
        ptpGroup = searchGroupInPtp(page, apikey, apiUser, isPopcorn)
        if (thisFetchTimeStart - triggerTime) < 30:
            time.sleep(40)
        if len(ptpGroup['Movies']) > 0:
            print('page: ', page, 'length: ', len(ptpGroup['Movies']))
            handleWithPtpGroupData(ptpGroup['Movies'])
            if (isPopcorn == False and int(page) < 100) or isPopcorn == True:
                fetchPtpGroups(page=str(int(page) + 1), triggerTime=thisFetchTimeStart, isPopcorn=isPopcorn)
    except Exception as e:
        print(e)
        time.sleep(60)
        if (isPopcorn == False and int(page) < 100) or isPopcorn == True:
            fetchPtpGroups(page=str(int(page) + 1), triggerTime=thisFetchTimeStart, isPopcorn=isPopcorn)

# 处理获取到的ptp group数据
def handleWithPtpGroupData(groupDataList):
    groupIds = []
    for i,singleGroupInfo in enumerate(groupDataList):
        groupIds.append(singleGroupInfo['GroupId'])
    # 对group数据进行本地比对
    idsObj = getPtpGroupsNotInMysql(groupIds=groupIds)
    # 新的group数据
    notExistedIds = idsObj["notExistedIds"]
    print('notExistedIds: ', len(idsObj["notExistedIds"]))
    dealWithNotExistedGroup(groupDataList, notExistedIds)
    # 已存在的group数据
    existedIds = idsObj["existedIds"]
    dealWithExistedGroup(groupDataList, existedIds)

# 新的group数据进行插入处理
def dealWithNotExistedGroup(groups, non_existing_group_ids):
    dataToInserter = []
    dataToInserterInRaw = []
    for i,singleGroupInfo in enumerate(groups):
        if singleGroupInfo['GroupId'] in non_existing_group_ids:
            if len(singleGroupInfo['Torrents']) > 0:
                isWebExist = 0
                isMkvExist = 0
                isBDExist = 0
                size=0
                torrentNumber = len(singleGroupInfo['Torrents'])
                BDSeederNumber = 0
                totalSeedNumber = 0
                hasGoldenPopcorn = 0
                for torrent in singleGroupInfo['Torrents']:
                    totalSeedNumber += int(torrent.get('Seeders', 0))
                    if torrent['Source'] == 'WEB':
                        isWebExist = 1
                    if torrent['Container'] == 'MKV':
                        isMkvExist = 1
                    if torrent['Container'] == 'm2ts':
                        isBDExist = 1
                        size = int(torrent.get('Size', 0))  # 修改: 确保 Size 是整数
                        BDSeederNumber = int(torrent.get('Seeders', 0))
                    if torrent["GoldenPopcorn"] == True:
                        hasGoldenPopcorn = 1
                ImdbId = singleGroupInfo.get('ImdbId', '')
                ImdbRating = singleGroupInfo.get('ImdbRating', '')
                ImdbVoteCount = int(singleGroupInfo.get('ImdbVoteCount', 0))
                dataToInserter.append((
                    singleGroupInfo['GroupId'],
                    singleGroupInfo['Title'],
                    json.dumps(singleGroupInfo),
                    isWebExist,
                    singleGroupInfo['Year'],
                    isMkvExist,
                    isBDExist,
                    size,
                    torrentNumber,
                    BDSeederNumber,
                    ImdbId,
                    ImdbRating,
                    ImdbVoteCount,
                    totalSeedNumber,
                    hasGoldenPopcorn
                ))
                dataToInserterInRaw.append((
                    singleGroupInfo['GroupId'],
                    json.dumps(singleGroupInfo),
                ))
    print('insertPtpGroupToMysql number: ', len(dataToInserter))
    insertPtpGroupToMysql(dataToInserter)
    print('insertPtpGroupToMysqlInRaw number: ', len(dataToInserterInRaw))
    insertPtpGroupToMysqlInRaw(dataToInserterInRaw)

# 已存在的group数据进行必要的update处理
def dealWithExistedGroup(groups, existing_group_ids):
    dataToInserter = []
    dataToInserterInRaw = []
    for i,singleGroupInfo in enumerate(groups):
        if singleGroupInfo['GroupId'] in existing_group_ids:
            if len(singleGroupInfo['Torrents']) > 0:
                isWebExist = 0
                isMkvExist = 0
                isBDExist = 0
                torrentNumber = len(singleGroupInfo['Torrents'])
                BDSeederNumber = 0
                totalSeedNumber = 0
                hasGoldenPopcorn = 0
                for torrent in singleGroupInfo['Torrents']:
                    totalSeedNumber += int(torrent.get('Seeders', 0))
                    if torrent['Source'] == 'WEB':
                        isWebExist = 1
                    if torrent['Container'] == 'MKV':
                        isMkvExist = 1
                    if torrent['Container'] == 'm2ts':
                        isBDExist = 1
                        BDSeederNumber = int(torrent.get('Seeders', 0))
                    if torrent["GoldenPopcorn"] == True:
                        hasGoldenPopcorn = 1
                ImdbId = singleGroupInfo.get('ImdbId', '')
                dataToInserter.append({
                    "groupId": singleGroupInfo['GroupId'],
                    "title": singleGroupInfo['Title'],
                    "groupInfo": json.dumps(singleGroupInfo),
                    "isWebExist": isWebExist,
                    "isMkvExist": isMkvExist,
                    "isBDExist": isBDExist,
                    "torrentNumber": torrentNumber,
                    "BDSeederNumber": BDSeederNumber,
                    "ImdbId": ImdbId,
                    "totalSeedNumber": totalSeedNumber,
                    "hasGoldenPopcorn": hasGoldenPopcorn
                })
                dataToInserterInRaw.append({
                    "groupId": singleGroupInfo['GroupId'],
                    "groupInfo": json.dumps(singleGroupInfo),
                })
    batchUpdatePtpGroupInfoWithMysql(dataToInserter)
    batchUpdatePtpGroupInfoWithMysqlInRaw(dataToInserterInRaw)

# 下载ptp种子
def downloadTorrentFile(id, dest='D:/shared'):
    apikey = 'PfCNmsaxquORb5uKEq4BKOHdcczHPnuv'
    apiUser = 'e7vUjD0Rd0yF5iUh'
    # dest = 'D:/shared'
    # dest = '/home/download'
    downloadTorrentFileInPtpById(id, dest, apikey, apiUser)

# 为有imdb的ptp种子添加相关信息，如国家等
@asyncz
def getImdbInfoForAllGroup(number):
    ptpGroups = getPtpGroupsWithImdb(pageSize=500, pageNumber=number)
    for i,singleGroupInfo in enumerate(ptpGroups["list"]):
        try:
            res = getInfoFroImdbById('tt' + singleGroupInfo["ImdbId"])
            print('page:',number,' 总数：', len(ptpGroups["list"]), " 当前序号:", i+1, " groupId:",singleGroupInfo["groupId"], res["country"], res["rating"]["ratingValue"], res["rating"]["ratingCount"])
            updatePtpGroupWithMysql(groupId=singleGroupInfo["groupId"], ImdbId=None, ImdbRating=res["rating"]["ratingValue"], ImdbVoteCount=res["rating"]["ratingCount"], country=res["country"])
        except Exception as e:
            print(e)
            print('异常number: ', number)
            time.sleep(2)

# 为所有group信息查找imdb信息，补全
def getImdbIdForAllGroup():
    dataToInserter = []
    ptpGroups = getPtpGroupsWithoutImdb()
    for i,singleGroupInfo in enumerate(ptpGroups["list"]):
        print('总数：', ptpGroups["total"], " 当前序号:", i+1, " groupId:",singleGroupInfo["groupId"])
        groupInfo = json.loads(singleGroupInfo["groupInfo"])
        print(groupInfo.get("ImdbId"))
        if groupInfo.get("ImdbId") != None:
            dataToInserter.append({
                "groupId": singleGroupInfo["groupId"],
                "ImdbId": groupInfo.get("ImdbId")
            })
    print(len(dataToInserter))
    batchUpdatePtpGroupInfoWithMysql(dataToInserter)

# 为所有group信息添加torrent信息
def getTorrentsForAllGroup():
    torrentsForInsert = []
    ptpGroups = getPtpGroupsFromMysql()
    print('总数：', len(ptpGroups["list"]))
    for i,singleGroupInfo in enumerate(ptpGroups["list"]):
        groupInfo = json.loads(singleGroupInfo["groupInfo"])
        if len(groupInfo['Torrents']) > 0:
            for torrent in groupInfo['Torrents']:
                isGoldPopcorn = 0
                if torrent["GoldenPopcorn"] == True:
                    isGoldPopcorn = 1
                torrentsForInsert.append((
                    singleGroupInfo["groupId"],
                    torrent["Id"],
                    torrent["Size"],
                    torrent["Seeders"],
                    torrent["Container"],
                    isGoldPopcorn
                ))
    print('种子总数: ', len(torrentsForInsert))
    insertPtpGroupTorrentFileInfoToMysql(torrentsForInsert)

# 为所有group信息添加torrent信息
def updateSeederNumberForAllGroup():
    dataToInserter = []
    ptpGroups = getPtpGroupsFromMysql()
    print('总数：', len(ptpGroups["list"]))
    for i,singleGroupInfo in enumerate(ptpGroups["list"]):
        groupInfo = json.loads(singleGroupInfo["groupInfo"])
        totalSeedNumber = 0
        if len(groupInfo['Torrents']) > 0:
            for torrent in groupInfo['Torrents']:
                totalSeedNumber += int(torrent.get('Seeders', 0))
        dataToInserter.append({
            "groupId": singleGroupInfo['groupId'],
            "totalSeedNumber": totalSeedNumber
        })
    print('dataToInserter数量:', len(dataToInserter))
    batchUpdatePtpGroupInfoWithMysql(dataToInserter)

# 为有imdb的ptp种子添加相关信息，如国家等
@asyncz
def getTmdbInfoForAllGroup(number):
    ptpGroups = getPtpGroupsWithImdb(pageSize=4400, pageNumber=number)
    for i,singleGroupInfo in enumerate(ptpGroups["list"]):
        try:
            res = getTmdbInfoFroImdbById('tt' + singleGroupInfo["ImdbId"])
            movieInfo = {}
            if len(res['movie_results']) > 0:
                movieInfo = res['movie_results'][0]
                print('page:',number,' 总数：', len(ptpGroups["list"]), " 当前序号:", i+1, " groupId:",singleGroupInfo["groupId"], movieInfo["id"], movieInfo["vote_count"], movieInfo["vote_average"])
                updatePtpGroupWithMysql(groupId=singleGroupInfo["groupId"], tmdbTitle= movieInfo['title'],tmdbId=movieInfo["id"], tmdbRating=movieInfo["vote_average"], tmdbVoteCount=movieInfo["vote_count"])
            else:
                print('page:',number,' 总数：', len(ptpGroups["list"]), " 当前序号:", i+1)
        except Exception as e:
            print(e)
            print('异常number: ', number)
            time.sleep(2)

def getBluMovieTorrents(pageNumber):
    dataTodo = getPtpGroupsWithTmdb(pageSize=27500, pageNumber=pageNumber)
    for i,group in enumerate(dataTodo['list']):
        try:
            res = getTorrentListInfoInBlu(group['tmdbId'])
            print('总数：', len(dataTodo['list']), "pageNumber:", pageNumber, " 当前序号:", i+1, " groupId:", group["groupId"], 'Blu res number:', len(res['data']))
            time.sleep(5 + 5 * random.random())
            if len(res['data']) > 0:
                dataToInsert = []
                for torrent in res['data']:
                    dataToInsert.append((
                        # torrentId, name, bdInfo, description, size, ImdbId, tmdbId, seeders, releaseYear, resolution, genres, poster
                        torrent['id'],
                        torrent['attributes']['name'],
                        torrent['attributes']['bd_info'],
                        torrent['attributes']['media_info'],
                        torrent['attributes']['description'],
                        torrent['attributes']['category_id'],
                        torrent['attributes']['type_id'],
                        torrent['attributes']['resolution_id'],
                        torrent['attributes']['region_id'] if 'region_id' in torrent.get('attributes', {}) else 0,
                        torrent['attributes']['distributor_id'] if 'distributor_id' in torrent.get('attributes', {}) else 0,
                        torrent['attributes']['size'],
                        torrent['attributes']['imdb_id'],
                        torrent['attributes']['tmdb_id'],
                        torrent['attributes']['seeders'],
                        torrent['attributes']['release_year'],
                        torrent['attributes']['resolution'],
                        torrent['attributes']['meta']['genres'],
                        torrent['attributes']['meta']['poster'], 
                    ))
                print('insertBluTorrentToMysql number: ', len(dataToInsert))
                insertBluTorrentToMysql(dataToInsert)
        except Exception as e:
            print(e)
            time.sleep(10)

@server.route('/uploadSourceTorrentInfo', methods=['get', 'post'])
def uploadSourceTorrentInfo():
    torrentId = request.values.get('torrentId')
    name = request.values.get('name')
    sourceSite = request.values.get('sourceSite')
    bdInfo = request.values.get('bdInfo')
    mediaInfo = request.values.get('mediaInfo')
    description = request.values.get('description')
    categoryId = request.values.get('categoryId')
    typeId = request.values.get('typeId')
    resolutionId = request.values.get('resolutionId')
    regionId = request.values.get('regionId')
    distributorId = request.values.get('distributorId')
    size = request.values.get('size')
    imdbId = request.values.get('imdbId')
    tmdbId = request.values.get('tmdbId')
    seeders = request.values.get('seeders')
    releaseYear = request.values.get('releaseYear')
    resolution = request.values.get('resolution')
    genres = request.values.get('genres')
    poster = request.values.get('poster')
    dataToInsert = []
    dataToInsert.append((
        torrentId, name, sourceSite, bdInfo, mediaInfo, description, categoryId, typeId, resolutionId, regionId, distributorId, size, imdbId, tmdbId, seeders, releaseYear, resolution, genres, poster
    ))
    print('insertSourceTorrentToMysql number: ', len(dataToInsert))
    insertSourceTorrentToMysql(dataToInsert)
    return '保存成功'

@server.route('/updateDownloadStatus', methods=['get', 'post'])
def updateDownloadStatus():
    torrentId = request.values.get('torrentId')
    isDownloaded = request.values.get('isDownloaded')
    batchUpdateBluTorrentsInfoWithMysql([{
        'torrentId': torrentId,
        'isDownloaded': isDownloaded,
    }])
    return 'torrent下载更新指令已下发'

@server.route('/updateUpgradeStatus', methods=['get', 'post'])
def updateUpgradeStatus():
    # torrentId, name, isUploaded, uploadSiteTorrentId, uploadSiteUrl
    torrentId = request.values.get('torrentId')
    sourceSite = request.values.get('sourceSite')
    sourceId = request.values.get('sourceId')
    name = request.values.get('name')
    isUploaded = request.values.get('isUploaded')
    uploadSiteTorrentId = request.values.get('uploadSiteTorrentId')
    uploadSiteUrl = request.values.get('uploadSiteUrl')
    if sourceSite == 'movies':
        data = [(
            torrentId,
            sourceId,
            name,
            isUploaded,
            uploadSiteTorrentId,
            uploadSiteUrl
        )]
        insertSourceSiteTorrentStatusToMysql(data)
    else:
        data = [(
            torrentId,
            name,
            isUploaded,
            uploadSiteTorrentId,
            uploadSiteUrl
        )]
        insertBluTorrentStatusToMysql(data)
    return 'torrent发布更新指令已下发'

def uploadBluTorrentToMyU3d(pageNumber = 1):
    bluList = getBluTorrentsFromMysql(pageSize=1, pageNumber=pageNumber)
    print(len(bluList['list']), ' torrentId:',bluList['list'][0]['torrentId'])
    torrentFile = getTorrentFileInBlu(bluList['list'][0]['torrentId'])
    print('torrent file downloaded')
    time.sleep(6)
    params = {
        'torrent': torrentFile,
        'name': bluList['list'][0]['name'],
        'description': bluList['list'][0]['description'],
        'mediainfo': bluList['list'][0]['mediaInfo'],
        'bdinfo': bluList['list'][0]['bdInfo'],
        'category_id': bluList['list'][0]['categoryid'],
        'type_id': bluList['list'][0]['typeId'],
        'resolution_id': bluList['list'][0]['resolutionId'],
        'region_id': bluList['list'][0]['regionId'],
        'distributor_id': bluList['list'][0]['distributorId'],
        'tmdb': bluList['list'][0]['tmdbId'],
        'imdb': bluList['list'][0]['ImdbId'],
        'tvdb': 0,
        'mal': 0,
        'igdb': 0,
        'anonymous': True,
        'stream': False,
        'sd': True,
        'personal_release': True,
        'mod_queue_opt_in': False,
        'anonymous': True,
    }
    res = uploadTorrentToBlu(params)
    print(res)

@server.route('/getTorrentFileLocally', methods=['get', 'post'])
def getTorrentFileLocally():
    # return '成功'
    sourceSite = request.values.get('sourceSite')
    torrentId = request.values.get('torrentId')
    torrent = get_torrent_by_id(torrentId)
    if not torrent:
        return abort(404, description="Torrent not found")
    if sourceSite:
        file_path = f"D:/shared/torrents/sourceTorrent/{sourceSite}_{torrent['file_name']}"
    else:
        file_path = f"D:/shared/torrents/bluTorrents/{torrent['file_name']}"
    return send_file(
        file_path,
        as_attachment=True,
    )


def get_torrent_by_id(torrent_id):
    # 根据ID获取种子文件信息的实际逻辑
    return {
        'file_name': f'{torrent_id}.torrent',
        'name': f'{torrent_id}'
    }

def sanitize_filename(filename):
    return filename.replace(' ', '.').replace('/', '-').replace('\\', '-')

@server.route('/getMoviesForMySite', methods=['get', 'post'])
def getMoviesForMySite():
    sourceSite = request.values.get('sourceSite')
    isUploaded = request.values.get('isUploaded')
    isDownloaded = request.values.get('isDownloaded')
    pageSize = request.values.get('pageSize')
    pageNumber = request.values.get('pageNumber')
    sortBy = request.values.get('sortBy')
    if sortBy == None:
        sortBy = 'seeders'
    list = {}
    if sourceSite == 'ptp':
        list = {}
    elif sourceSite == 'movies':
        list = getSourceSiteTorrentsFromMysql(pageSize=int(pageSize), pageNumber=int(pageNumber), isUploaded=isUploaded, isDownloaded=isDownloaded, sortBy=sortBy)
    else:
        list = getBluTorrentsFromMysql(pageSize=int(pageSize), pageNumber=int(pageNumber), isUploaded=isUploaded, isDownloaded=isDownloaded, sortBy=sortBy)
    print(list['total'])
    return list

@server.route('/getAllBluMovies', methods=['get', 'post'])
def getAllBluMovies():
    pageSize = request.values.get('pageSize')
    pageNumber = request.values.get('pageNumber')
    sortBy = request.values.get('sortBy')
    sortOrder = request.values.get('sortOrder')
    list = getAllBluTorrentsFromMysql(pageSize=int(pageSize), pageNumber=int(pageNumber), sortBy=sortBy,sortOrder=sortOrder)
    print(list['total'])
    return list

def transferAlbumsFromSqlite():
    albums = getAlbums()
    print(albums['total'])
    dataToInserter = []
    for i,album in enumerate(albums['list']):
        dataToInserter.append((
            album['qobuzId'],
            album['albumInfo'],
            album['albumTitle'],
            album['isHiRes'],
            album['trackNumber'],
            album['downloadStatusFor16bit'],
            album['downloadStatusFor24bit'],
            album['downloadStatusForMp3320'],
            album['downloadStartTime'],
            album['fileDirFor24bit'],
            album['fileDirFor16bit'],
            album['fileDirForMp3320'],
            album['coverUrl'],
            album['coverUrlTranslate'],
            album['spectrogramUrlFor24bit'],
            album['spectrogramUrlFor16bit'],
            album['spectrogramUrlForMp3320'],
            album['releaseTime']
        ))
    print('insertAllAlbumsToMysqlFromSqlite number: ', len(dataToInserter))
    insertAllAlbumsToMysqlFromSqlite(dataToInserter)

def transferAlbumsStatusFromSqlite():
    albums = getPtAlbums(ptWeb="ops")
    print(albums['total'])
    dataToInserter = []
    for i,album in enumerate(albums['list']):
        dataToInserter.append((
            album['qobuzId'],
            album['uploadedStatus'],
        ))
    print('insertAlbumsForOpsToMysql number: ', len(dataToInserter))
    # insertAlbumsForOpsToMysql(dataToInserter)
    batchInsertAlbumStatusToMysql(data=dataToInserter, tableName='albumStatusForPtSiteForOps')

    albums = getPtAlbums()
    print(albums['total'])
    dataToInserter = []
    for i,album in enumerate(albums['list']):
        dataToInserter.append((
            album['qobuzId'],
            album['uploadedStatus'],
        ))
    print('insertAlbumsForRedToMysql number: ', len(dataToInserter))
    # insertAlbumsForRedToMysql(dataToInserter)
    batchInsertAlbumStatusToMysql(data=dataToInserter, tableName='albumStatusForPtSiteForRed')

def transferAlbumsFromMysql():
    albums = getAlbumsFromMysql()
    print(albums['total'])
    dataToInserter = []
    for i,album in enumerate(albums['list']):
        dataToInserter.append((
            album['qobuzId'],
            album['albumInfo'],
            album['albumTitle'],
            album['isHiRes'],
            album['trackNumber'],
            album['downloadStatusFor16bit'],
            album['downloadStatusFor24bit'],
            album['downloadStatusForMp3320'],
            album['downloadStartTime'],
            album['fileDirFor24bit'],
            album['fileDirFor16bit'],
            album['fileDirForMp3320'],
            album['coverUrl'],
            album['coverUrlTranslate'],
            album['spectrogramUrlFor24bit'],
            album['spectrogramUrlFor16bit'],
            album['spectrogramUrlForMp3320'],
            album['releaseTime']
        ))
    print('insertAllAlbumsToSqliteFromMysql number: ', len(dataToInserter))
    # insertAllAlbumsToMysqlFromSqlite(dataToInserter)
    # insertAlbum(albumInQobuz["id"],json.dumps(albumDetail),albumInQobuz["title"],albumInQobuz["release_date_original"],isHiRes,albumDetail["tracks_count"])
    insertAlbums(dataToInserter)

def transferAlbumsStatusFromMysql():
    albums = getAlbumsWithStatusFromMysql(ptType="Ops")
    print(albums['total'])
    dataToInserter = []
    for i,album in enumerate(albums['list']):
        dataToInserter.append((
            album['qobuzId'],
            album['uploadedStatus'],
        ))
    print('insertAlbumsForOpsToMysql number: ', len(dataToInserter))
    # insertAlbumsForOpsToMysql(dataToInserter)
    batchInsertAlbumStatusToMysql(data=dataToInserter, tableName='albumStatusForPtSiteForOps')

    albums = getPtAlbums()
    print(albums['total'])
    dataToInserter = []
    for i,album in enumerate(albums['list']):
        dataToInserter.append((
            album['qobuzId'],
            album['uploadedStatus'],
        ))
    print('insertAlbumsForRedToMysql number: ', len(dataToInserter))
    # insertAlbumsForRedToMysql(dataToInserter)
    batchInsertAlbumStatusToMysql(data=dataToInserter, tableName='albumStatusForPtSiteForRed')



class RepeatingTimer(Timer): 
    def run(self):
        while not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
            self.finished.wait(self.interval)

if __name__ == '__main__':
    # create_albums()
    # create_redAlbums()
    # create_PtAlbums("pterWeb")
    # create_PtAlbums("opencd")
    # create_PtAlbums("ggn")
    # create_PtAlbums("ops")
    # create_PtAlbums("tjupt")
    # create_PtAlbums("dic")
    # fetchPtpGroups()
    # downloadTorrentFile('1315882')
    # getImdbInfoForAllGroup(1)
    # getImdbIdForAllGroup()
    # getTorrentsForAllGroup()
    # getTmdbInfoForAllGroup(1)
    # getTmdbInfoForAllGroup(2)
    # getTmdbInfoForAllGroup(3)
    # getTmdbInfoForAllGroup(4)
    # getTmdbInfoForAllGroup(5)
    # getTmdbInfoForAllGroup(6)
    # getTmdbInfoForAllGroup(7)
    # getTmdbInfoForAllGroup(8)
    # getTmdbInfoForAllGroup(9)
    # getTmdbInfoForAllGroup(10)
    # getBluMovieTorrents(9)
    # getTorrentFileInBlu('63285')
    # getBluTorrentFile(1)
    # getTorrentFileLocally('96350')
    # updateSeederNumberForAllGroup()
    # res = getInfoFroImdbById("tt32613601")
    # print(res)
    # transferAlbums()
    # transferAlbumsStatus()
    t = RepeatingTimer(10.0, checkUndownloadedAlbums)
    t.start()
    server.run(debug=False, port=443, host='0.0.0.0',ssl_context=('/home/server/cert/server.crt','/home/server/cert/server.key'))# 指定端口、host,0.0.0.0代表不管几个网卡，任何ip都可以访问
