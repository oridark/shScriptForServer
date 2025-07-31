#!/bin/bash

# 检查是否提供了目录路径
if [ -z "$1" ]; then
    echo "使用方法: $0 <包含FLAC音乐的目录>"
    echo "例如: $0 \"Domain - Embodiment of Fear (2020) [24B-44.1kHz]\""
    exit 1
fi

# --- 1. 安装必要的工具 ---
# 确保 ffmpeg 和 realpath 工具已安装
if ! command -v ffmpeg &> /dev/null || ! command -v realpath &> /dev/null; then
    echo "正在检查并安装必要的工具 (ffmpeg, realpath)..."
    sudo apt update
    sudo apt install -y ffmpeg realpath
    if ! command -v ffmpeg &> /dev/null || ! command -v realpath &> /dev/null; then
        echo "错误：无法安装必要的工具 (ffmpeg 和 realpath)，请手动安装后重试。"
        exit 1
    fi
    echo "必要工具已安装完成。"
fi

# --- 2. 处理源目录路径 ---
SOURCE_DIR="$1"

# 使用 realpath -s 规范化源路径，确保是绝对路径且没有符号链接
SOURCE_DIR=$(realpath -s "$SOURCE_DIR")
SOURCE_DIR="${SOURCE_DIR%/}/" # 确保以斜杠结尾

# 从 SOURCE_DIR 中提取目录名（文件名部分）
folder_name=$(basename "${SOURCE_DIR%/}")

# 替换文件名中最后一个方括号标记为 "[MP3-V0]"
TARGET_FOLDER_NAME=$(echo "$folder_name" | sed -E 's/\[[^]]*\]$/[MP3-V0]/')

# 构建目标目录的完整路径
# 先获取源目录的父目录
parent_dir=$(dirname "${SOURCE_DIR%/}")
TARGET_DIR="${parent_dir}/${TARGET_FOLDER_NAME}"

echo "源目录: $SOURCE_DIR"
echo "目标目录: $TARGET_DIR"

# 创建目标目录
mkdir -p "$TARGET_DIR"

# --- 3. 复制非 FLAC 文件和目录结构 ---
echo "正在从源目录复制所有非FLAC文件和目录结构到目标目录..."
# 使用 rsync 复制除 *.flac 文件外的所有内容
# -a: 归档模式，保持权限、时间戳等
# --exclude='*.flac': 排除 FLAC 文件，不复制
# --exclude='.': 排除源目录本身，避免复制到目标目录中，造成一个目录嵌套
# --include='*/': 包含目录以便递归复制
rsync -a --exclude='*.flac' "$SOURCE_DIR" "$TARGET_DIR"
echo "非FLAC文件和目录结构已复制完成。"

# --- 4. 转换所有 FLAC 文件 ---
echo "开始转换 FLAC 文件..."
# 使用 find -exec 处理 FLAC 文件，避免处理文件名中的特殊字符
find "$SOURCE_DIR" -type f -name "*.flac" -exec bash -c '
    flac_file="$1"
    SOURCE_DIR="$2"
    TARGET_DIR="$3"

    # 使用 realpath --relative-to 获取 FLAC 文件相对于 SOURCE_DIR 的路径
    relative_flac_path=$(realpath --relative-to="$SOURCE_DIR" "$flac_file")
    
    # 获取文件名（包含路径）但不包含扩展名
    filename_without_ext="${relative_flac_path%.flac}"
    
    # 构建目标 MP3 文件的完整路径
    target_mp3_path="$TARGET_DIR/${filename_without_ext}.mp3"
    
    # 确保目标 MP3 文件所在的目录存在
    mkdir -p "$(dirname "$target_mp3_path")"
    
    echo "正在转换: $flac_file 到 $target_mp3_path"
    
    # 使用 ffmpeg 转换，同时转换所有元数据和封面
    ffmpeg -i "$flac_file" -q:a 0 -map_metadata 0 -id3v2_version 3 -write_id3v1 1 "$target_mp3_path" -y
    
    if [ $? -eq 0 ]; then
        echo "成功转换: $flac_file"
    else
        echo "转换失败: $flac_file"
    fi
' _ {} "$SOURCE_DIR" "$TARGET_DIR" \;

echo "所有 FLAC 文件转换处理完成。"

# --- 5. 创建 .torrent 文件 ---
# 确保 mktorrent 工具已安装 (如果需要创建 torrent 文件)
if ! command -v mktorrent &> /dev/null; then
    echo "mktorrent 未安装，正在尝试安装..."
    sudo apt install -y mktorrent
    if ! command -v mktorrent &> /dev/null; then
        echo "警告：无法安装 mktorrent，跳过 .torrent 文件创建。"
    else
        # 替换为常用的公共 Tracker URL
        mktorrent -v -p -d -l 18 -a "https://0" -o "${TARGET_DIR}.torrent" "${TARGET_DIR}"
        echo "已为 $TARGET_DIR 创建 .torrent 文件: ${TARGET_DIR}.torrent"
    fi
else
    # 替换为常用的公共 Tracker URL
    mktorrent -v -p -d -l 18 -a "https://0" -o "${TARGET_DIR}.torrent" "${TARGET_DIR}"
    echo "已为 $TARGET_DIR 创建 .torrent 文件: ${TARGET_DIR}.torrent"
fi

echo "所有 FLAC 文件转换完成，MP3 V0 文件保存在 $TARGET_DIR"
echo "原始专辑目录中的所有其他文件也已复制完成。"