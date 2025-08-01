import logging
import os, time, sqlite3
import mysql.connector
from typing import Tuple
import requests
from pathvalidate import sanitize_filename, sanitize_filepath
from tqdm import tqdm
import json

import qobuz_dl.metadata as metadata
from qobuz_dl.color import OFF, GREEN, RED, YELLOW, CYAN
from qobuz_dl.exceptions import NonStreamable

QL_DOWNGRADE = "FormatRestrictedByFormatAvailability"
# used in case of error
DEFAULT_FORMATS = {
    "MP3": [
        # "{artist} - {album} ({year}) [MP3] [320]",
        "{artist} - {album} ({year}) [MP3-320]",
        "{tracknumber}. {tracktitle}",
    ],
    "Unknown": [
        "{artist} - {album}",
        "{tracknumber}. {tracktitle}",
    ],
}

DEFAULT_FOLDER = "{artist} - {album} ({year}) [{bit_depth}B-{sampling_rate}kHz]"
DEFAULT_FOLDER_OST = "{artist} - {album} ({year}) [FLAC] [{bit_depth}]"
DEFAULT_TRACK = "{tracknumber}. {tracktitle}"

logger = logging.getLogger(__name__)


class Download:
    def __init__(
        self,
        client,
        item_id: str,
        path: str,
        quality: int,
        embed_art: bool = False,
        albums_only: bool = False,
        downgrade_quality: bool = False,
        cover_og_quality: bool = False,
        no_cover: bool = False,
        folder_format=None,
        track_format=None,
    ):
        self.client = client
        self.item_id = item_id
        self.path = path
        self.quality = quality
        self.albums_only = albums_only
        self.embed_art = embed_art
        self.downgrade_quality = downgrade_quality
        self.cover_og_quality = cover_og_quality
        self.no_cover = no_cover
        self.folder_format = folder_format or DEFAULT_FOLDER
        self.track_format = track_format or DEFAULT_TRACK

    def download_id_by_type(self, track=True):
        if not track:
            self.download_release()
        else:
            self.download_track()

    def download_release(self):
        count = 0
        meta = self.client.get_album_meta(self.item_id)
        if not meta.get("streamable"):
            raise NonStreamable("This release is not streamable")

        if self.albums_only and (
            meta.get("release_type") != "album"
            or meta.get("artist").get("name") == "Various Artists"
        ):
            logger.info(f'{OFF}Ignoring Single/EP/VA: {meta.get("title", "n/a")}')
            return

        album_title = _get_title(meta)

        format_info = self._get_format(meta)
        file_format, quality_met, bit_depth, sampling_rate = format_info

        if not self.downgrade_quality and not quality_met:
            logger.info(
                f"{OFF}Skipping {album_title} as it doesn't meet quality requirement"
            )
            return

        logger.info(
            f"\n{YELLOW}Downloading: {album_title}\nQuality: {file_format}"
            f" ({bit_depth}/{sampling_rate})\n"
        )
        album_attr = self._get_album_attr(
            meta, album_title, file_format, bit_depth, sampling_rate
        )
        # if "Original Game Soundtrack" in album_title:
        #     self.folder_format = DEFAULT_FOLDER_OST
        folder_format, track_format = _clean_format_str(
            self.folder_format, self.track_format, file_format
        )
        sanitized_title = sanitize_filepath(folder_format.format(**album_attr))
        dirn = os.path.join(self.path, sanitized_title)
        os.makedirs(dirn, exist_ok=True)

        if self.no_cover:
            logger.info(f"{OFF}Skipping cover")
        else:
            _get_extra(meta["image"]["large"], dirn, og_quality=self.cover_og_quality)

        if "goodies" in meta:
            try:
                _get_extra(meta["goodies"][0]["url"], dirn, "booklet.pdf")
            except:  # noqa
                pass
        media_numbers = [track["media_number"] for track in meta["tracks"]["items"]]
        is_multiple = True if len([*{*media_numbers}]) > 1 else False
        for i in meta["tracks"]["items"]:
            parse = self.client.get_track_url(i["id"], fmt_id=self.quality)
            if "sample" not in parse and parse["sampling_rate"]:
                is_mp3 = True if int(self.quality) == 5 else False
                self._download_and_tag(
                    dirn,
                    count,
                    parse,
                    i,
                    meta,
                    False,
                    is_mp3,
                    i["media_number"] if is_multiple else None,
                )
            else:
                logger.info(f"{OFF}Demo. Skipping")
            count = count + 1
        logger.info(f"{GREEN}Completed")
        if self.quality != 5:
            if is_multiple:
                os.system(f"sox \"{dirn}\"/Disc*/*flac -n spectrogram -o \"{self.path}\"/\"{sanitized_title}\"_spectrogram.png")
            else:
                os.system(f"sox \"{dirn}\"/*flac -n spectrogram -o \"{self.path}\"/\"{sanitized_title}\"_spectrogram.png")
        os.system(f"mktorrent -v -p -d -l 18 -a https://0 -o \"{dirn}.torrent\" \"{dirn}\"")
        # if self.quality != 5:
        #     with sqlite3.connect("/home/server/checkQobuzRepeat.db", timeout=10, check_same_thread=False) as conn:
        #         conn.row_factory = dict_factory
        #         cur = conn.cursor()
        #         dirPath = dirn.split('kHz]')[0] + 'kHz]'
        #         try:
        #             checkAlbums = cur.execute('''select * from albums where qobuzId=?''',(self.item_id,)).fetchall()
        #             if len(checkAlbums) > 0:
        #                 if (self.quality == 27):
        #                     if checkAlbums[0]['downloadStatusFor24bit'] == '2':
        #                         return
        #                     cur.execute("""UPDATE albums SET downloadStatusFor24bit=?, fileDirFor24bit=? where qobuzId=?;""", ("1", dirPath, self.item_id,))
        #                 elif(self.quality == 6):
        #                     if checkAlbums[0]['downloadStatusFor16bit'] == '2':
        #                         return
        #                     cur.execute("""UPDATE albums SET downloadStatusFor16bit=?, fileDirFor16bit=? where qobuzId=?;""", ("1", dirPath, self.item_id,))
        #                 elif(self.quality == 5):
        #                     if checkAlbums[0]['downloadStatusForMp3320'] == '2':
        #                         return
        #                     cur.execute("""UPDATE albums SET downloadStatusForMp3320=?, fileDirForMp3320=? where qobuzId=?;""", ("1", dirPath, self.item_id,))
        #                 conn.commit()
        #                 logger.info(f"{YELLOW}Download Status Updated")
        #         except sqlite3.OperationalError:
        #             pass
        checkAlbums = getAlbumsFromMysql(qobuzId=self.item_id)["list"]
        dirPath = dirn.split('kHz]')[0] + 'kHz]'
        if len(checkAlbums) > 0:
            if (self.quality == 27):
                if checkAlbums[0]['downloadStatusFor24bit'] == '2':
                    return
                batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': self.item_id, 'downloadStatusFor24bit': '1', 'fileDirFor24bit': dirPath}])
            elif(self.quality == 6):
                if checkAlbums[0]['downloadStatusFor16bit'] == '2':
                    return
                batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': self.item_id, 'downloadStatusFor16bit': '1', 'fileDirFor16bit': dirPath}])
            elif(self.quality == 5):
                if checkAlbums[0]['downloadStatusForMp3320'] == '2':
                    return
                batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': self.item_id, 'downloadStatusForMp3320': '1', 'fileDirForMp3320': dirPath}])
            

    def download_track(self):
        parse = self.client.get_track_url(self.item_id, self.quality)

        if "sample" not in parse and parse["sampling_rate"]:
            meta = self.client.get_track_meta(self.item_id)
            track_title = _get_title(meta)
            artist = _safe_get(meta, "performer", "name")
            logger.info(f"\n{YELLOW}Downloading: {artist} - {track_title}")
            format_info = self._get_format(meta, is_track_id=True, track_url_dict=parse)
            file_format, quality_met, bit_depth, sampling_rate = format_info

            folder_format, track_format = _clean_format_str(
                self.folder_format, self.track_format, str(bit_depth)
            )

            if not self.downgrade_quality and not quality_met:
                logger.info(
                    f"{OFF}Skipping {track_title} as it doesn't "
                    "meet quality requirement"
                )
                return
            track_attr = self._get_track_attr(
                meta, track_title, bit_depth, sampling_rate
            )
            sanitized_title = sanitize_filepath(folder_format.format(**track_attr))

            dirn = os.path.join(self.path, sanitized_title)
            os.makedirs(dirn, exist_ok=True)
            if self.no_cover:
                logger.info(f"{OFF}Skipping cover")
            else:
                _get_extra(
                    meta["album"]["image"]["large"],
                    dirn,
                    og_quality=self.cover_og_quality,
                )
            is_mp3 = True if int(self.quality) == 5 else False
            self._download_and_tag(
                dirn,
                1,
                parse,
                meta,
                meta,
                True,
                is_mp3,
                False,
            )
        else:
            logger.info(f"{OFF}Demo. Skipping")
        logger.info(f"{GREEN}Completed")

    def _download_and_tag(
        self,
        root_dir,
        tmp_count,
        track_url_dict,
        track_metadata,
        album_or_track_metadata,
        is_track,
        is_mp3,
        multiple=None,
    ):
        extension = ".mp3" if is_mp3 else ".flac"

        try:
            url = track_url_dict["url"]
        except KeyError:
            logger.info(f"{OFF}Track not available for download")
            dirPath = root_dir.split('[16B')[0].split('[24B')[0]
            if (self.quality == 27):
                batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': self.item_id, 'downloadStatusFor24bit': '2', 'fileDirFor24bit': dirPath}])
            elif(self.quality == 6):
                batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': self.item_id, 'downloadStatusFor16bit': '2', 'fileDirFor16bit': dirPath}])
            elif(self.quality == 5):
                batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': self.item_id, 'downloadStatusForMp3320': '2', 'fileDirForMp3320': dirPath}])
            # with sqlite3.connect("/home/server/checkQobuzRepeat.db", timeout=10, check_same_thread=False) as conn:
            #     conn.row_factory = dict_factory
            #     cur = conn.cursor()
            #     try:
            #         if (self.quality == 27):
            #             cur.execute("""UPDATE albums SET downloadStatusFor24bit=?, fileDirFor24bit=? where qobuzId=?;""", ("2", dirPath, self.item_id,))
            #         elif(self.quality == 6):
            #             cur.execute("""UPDATE albums SET downloadStatusFor16bit=?, fileDirFor16bit=? where qobuzId=?;""", ("2", dirPath, self.item_id,))
            #         elif(self.quality == 5):
            #             cur.execute("""UPDATE albums SET downloadStatusForMp3320=?, fileDirForMp3320=? where qobuzId=?;""", ("2", dirPath, self.item_id,))
            #         conn.commit()
            #         # cur.execute("""UPDATE albums SET isDownloadWrong=?, fileDir=? where qobuzId=?;""", ("1", root_dir,self.item_id,))
            #         # conn.commit()
            #         logger.info(f"{YELLOW}DownloadedWrongStatus Status Updated")
            #     except sqlite3.OperationalError:
            #         pass
            return

        if multiple:
            root_dir = os.path.join(root_dir, f"Disc {multiple}")
            os.makedirs(root_dir, exist_ok=True)

        filename = os.path.join(root_dir, f".{tmp_count:02}.tmp")

        # Determine the filename
        track_title = track_metadata.get("title")
        if track_metadata.get("version") != None:
            track_title = track_title + '('+ track_metadata.get("version") +')'
        artist = _safe_get(track_metadata, "performer", "name")
        filename_attr = self._get_filename_attr(artist, track_metadata, track_title)

        # track_format is a format string
        # e.g. '{tracknumber}. {artist} - {tracktitle}'
        formatted_path = sanitize_filename(self.track_format.format(**filename_attr))
        final_file = os.path.join(root_dir, formatted_path)[:250] + extension

        if os.path.isfile(final_file):
            logger.info(f"{OFF}{track_title} was already downloaded")
            return
        try:
            tqdm_download(url, filename, filename)
            tag_function = metadata.tag_mp3 if is_mp3 else metadata.tag_flac
            tag_function(
                filename,
                root_dir,
                final_file,
                track_metadata,
                album_or_track_metadata,
                is_track,
                self.embed_art,
            )
        except Exception as e:
            dirPath = root_dir.split('[16B')[0].split('[24B')[0]
            if (self.quality == 27):
                batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': self.item_id, 'downloadStatusFor24bit': '2', 'fileDirFor24bit': dirPath}])
            elif(self.quality == 6):
                batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': self.item_id, 'downloadStatusFor16bit': '2', 'fileDirFor16bit': dirPath}])
            elif(self.quality == 5):
                batchUpdateAlbumInfoWithMysql(updates=[{'qobuzId': self.item_id, 'downloadStatusForMp3320': '2', 'fileDirForMp3320': dirPath}])
            # with sqlite3.connect("/home/server/checkQobuzRepeat.db", timeout=10, check_same_thread=False) as conn:
            #     conn.row_factory = dict_factory
            #     cur = conn.cursor()
            #     try:
            #         if (self.quality == 27):
            #             cur.execute("""UPDATE albums SET downloadStatusFor24bit=?, fileDirFor24bit=? where qobuzId=?;""", ("2", dirPath, self.item_id,))
            #         elif(self.quality == 6):
            #             cur.execute("""UPDATE albums SET downloadStatusFor16bit=?, fileDirFor16bit=? where qobuzId=?;""", ("2", dirPath, self.item_id,))
            #         elif(self.quality == 5):
            #             cur.execute("""UPDATE albums SET downloadStatusForMp3320=?, fileDirForMp3320=? where qobuzId=?;""", ("2", dirPath, self.item_id,))
            #         conn.commit()
            #         # cur.execute("""UPDATE albums SET isDownloadWrong=?, fileDir=? where qobuzId=?;""", ("1", root_dir,self.item_id,))
            #         # conn.commit()
            #         logger.info(f"{YELLOW}DownloadedWrongStatus Status Updated")
            #     except sqlite3.OperationalError:
            #         pass
            logger.error(f"{RED}Error tagging the file: {e}", exc_info=True)
    
    @staticmethod
    def _get_filename_attr(artist, track_metadata, track_title):
        return {
            "artist": artist,
            "albumartist": _safe_get(
                track_metadata, "album", "artist", "name", default=artist
            ),
            "bit_depth": track_metadata["maximum_bit_depth"],
            "sampling_rate": track_metadata["maximum_sampling_rate"],
            "tracktitle": track_title,
            "version": track_metadata.get("version"),
            "tracknumber": f"{track_metadata['track_number']:02}",
        }

    @staticmethod
    def _get_track_attr(meta, track_title, bit_depth, sampling_rate):
        return {
            "album": meta["album"]["title"],
            "artist": meta["album"]["artist"]["name"],
            "tracktitle": track_title,
            "year": meta["album"]["release_date_original"].split("-")[0],
            "bit_depth": bit_depth,
            "sampling_rate": sampling_rate,
        }

    @staticmethod
    def _get_album_attr(meta, album_title, file_format, bit_depth, sampling_rate):
        return {
            "artist": meta["artist"]["name"],
            "album": album_title,
            "year": meta["release_date_original"].split("-")[0],
            "format": file_format,
            "bit_depth": bit_depth,
            "sampling_rate": sampling_rate,
        }

    def _get_format(self, item_dict, is_track_id=False, track_url_dict=None):
        quality_met = True
        if int(self.quality) == 5:
            return ("MP3", quality_met, None, None)
        track_dict = item_dict
        if not is_track_id:
            track_dict = item_dict["tracks"]["items"][0]

        try:
            new_track_dict = (
                self.client.get_track_url(track_dict["id"], fmt_id=self.quality)
                if not track_url_dict
                else track_url_dict
            )
            restrictions = new_track_dict.get("restrictions")
            if isinstance(restrictions, list):
                if any(
                    restriction.get("code") == QL_DOWNGRADE
                    for restriction in restrictions
                ):
                    quality_met = False

            return (
                "FLAC",
                quality_met,
                new_track_dict["bit_depth"],
                new_track_dict["sampling_rate"],
            )
        except (KeyError, requests.exceptions.HTTPError):
            return ("Unknown", quality_met, None, None)

def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

def tqdm_download(url, fname, desc):
    r = requests.get(url, allow_redirects=True, stream=True)
    total = int(r.headers.get("content-length", 0))
    download_size = 0
    with open(fname, "wb") as file, tqdm(
        total=total,
        unit="iB",
        unit_scale=True,
        unit_divisor=1024,
        desc=desc,
        bar_format=CYAN + "{n_fmt}/{total_fmt} /// {desc}",
    ) as bar:
        for data in r.iter_content(chunk_size=1024):
            size = file.write(data)
            bar.update(size)
            download_size += size

    if total != download_size:
        # https://stackoverflow.com/questions/69919912/requests-iter-content-thinks-file-is-complete-but-its-not
        raise ConnectionError("File download was interrupted for " + fname)


def _get_description(item: dict, track_title, multiple=None):
    downloading_title = f"{track_title} "
    f'[{item["bit_depth"]}/{item["sampling_rate"]}]'
    if multiple:
        downloading_title = f"[Disc {multiple}] {downloading_title}"
    return downloading_title


def _get_title(item_dict):
    album_title = item_dict["title"]
    version = item_dict.get("version")
    if version:
        album_title = (
            f"{album_title} ({version})"
            if version.lower() not in album_title.lower()
            else album_title
        )
    return album_title


def _get_extra(item, dirn, extra="cover.jpg", og_quality=False):
    extra_file = os.path.join(dirn, extra)
    if os.path.isfile(extra_file):
        logger.info(f"{OFF}{extra} was already downloaded")
        return
    tqdm_download(
        item.replace("_600.", "_org.") if og_quality else item,
        extra_file,
        extra,
    )


def _clean_format_str(folder: str, track: str, file_format: str) -> Tuple[str, str]:
    """Cleans up the format strings, avoids errors
    with MP3 files.
    """
    final = []
    for i, fs in enumerate((folder, track)):
        if fs.endswith(".mp3"):
            fs = fs[:-4]
        elif fs.endswith(".flac"):
            fs = fs[:-5]
        fs = fs.strip()

        # default to pre-chosen string if format is invalid
        if file_format in ("MP3", "Unknown") and (
            "bit_depth" in fs or "sampling_rate" in fs
        ):
            default = DEFAULT_FORMATS[file_format][i]
            logger.error(
                f"{RED}invalid format string for format {file_format}"
                f". defaulting to {default}"
            )
            fs = default
        final.append(fs)

    return tuple(final)


def _safe_get(d: dict, *keys, default=None):
    """A replacement for chained `get()` statements on dicts:
    >>> d = {'foo': {'bar': 'baz'}}
    >>> _safe_get(d, 'baz')
    None
    >>> _safe_get(d, 'foo', 'bar')
    'baz'
    """
    curr = d
    res = default
    for key in keys:
        res = curr.get(key, default)
        if res == default or not hasattr(res, "__getitem__"):
            return res
        else:
            curr = res
    return res

def batchUpdateAlbumInfoWithMysql(updates=[], batch_size=1000):
    conn = None
    try:
        # 连接到数据库
        conn = mysql.connector.connect(
            host='152.53.246.221',
            port=48306,
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
            # host='127.0.0.1',
            host='152.53.246.221',
            port=48306,
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
