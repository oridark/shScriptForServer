#!/bin/bash

# ========================
# 批量音乐专辑 FLAC 转 MP3-V0 自动转换脚本 (最终优化版)
# 用法: ./batch_convert.sh <工作目录> <convert_music.sh绝对路径> [FORCE_RECONVERT]
# 例如: ./batch_convert.sh /home/download /home/download/convert_music.sh
# 强制重新转换所有FLAC: ./batch_convert.sh /home/download /home/download/convert_music.sh FORCE_RECONVERT
# ========================

# 1. 参数校验与环境准备
# 检查是否传入了工作目录和转换脚本路径
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "用法: $0 <工作目录> <convert_music.sh绝对路径> [FORCE_RECONVERT]"
    echo "例如: $0 /home/download /home/download/convert_music.sh"
    echo "强制重新转换所有FLAC: $0 /home/download /home/download/convert_music.sh FORCE_RECONVERT"
    exit 1
fi
WORKDIR="$1"
CONVERT_SCRIPT="$2"
FORCE_RECONVERT=${3:-"false"} # 第三个参数，如果存在且为 "FORCE_RECONVERT"，则强制重新转换

# 切换到工作目录，确保后续所有操作都在该目录下进行
cd "$WORKDIR" || { echo "错误: 无法进入 $WORKDIR 目录。请确保目录存在且有权限。"; exit 1; }

# 检查转换脚本是否存在且可执行
if [ ! -f "$CONVERT_SCRIPT" ]; then
    echo "错误: 转换脚本 '$CONVERT_SCRIPT' 未找到。请确保它在正确的路径下。"
    exit 1
fi
if [ ! -x "$CONVERT_SCRIPT" ]; then
    echo "错误: 转换脚本 '$CONVERT_SCRIPT' 没有执行权限。请运行 'chmod +x $CONVERT_SCRIPT'。"
    exit 1
fi

echo "开始批量处理音乐专辑 (最终优化版)..."
echo "工作目录: $WORKDIR"
echo "转换脚本: $CONVERT_SCRIPT"
if [ "$FORCE_RECONVERT" = "FORCE_RECONVERT" ]; then
    echo "模式: 强制重新转换所有符合条件的 FLAC 专辑。"
else
    echo "模式: 仅转换未存在对应 MP3-V0 版本的 FLAC 专辑。"
fi
echo "---------------------------------------------------"

# 函数：提取专辑的基础名（去除所有末尾的 [XXX] 标记）
# 例如 "Album Name [24B-FLAC]" -> "Album Name"
# "Album Name (Year) [Edition] [MP3-V0]" -> "Album Name (Year)"
get_base_name() {
    local dir_name="$1"
    # 使用正则表达式移除末尾的 [xxx] 标记，可以重复移除多个
    echo "$dir_name" | sed -E 's/(\s\[[^]]*\])+$//'
}

# 2. 收集当前目录下所有一级子目录名
all_dirs=()
while IFS= read -r dir; do
    all_dirs+=("$dir")
done < <(find . -maxdepth 1 -type d -printf '%f\n' | sed '/^\.$/d')

echo "--- 调试信息：所有一级子目录 ---"
printf '%s\n' "${all_dirs[@]}"
echo "--------------------------------"


# 3. 收集所有已存在的 MP3-V0 目录的基础名
declare -A existing_mp3_bases # 使用关联数组进行去重
if [ "$FORCE_RECONVERT" != "FORCE_RECONVERT" ]; then
    for dir in "${all_dirs[@]}"; do
        if [[ "$dir" =~ \[MP3-V0\]$ ]]; then
            base_name=$(get_base_name "$dir")
            if [[ -n "$base_name" ]]; then
                existing_mp3_bases["$base_name"]=1 # 存储基础名
            fi
        fi
    done
fi

echo "--- 调试信息：已识别的 MP3-V0 专辑基础名 (如果强制转换，此列表为空) ---"
if [ ${#existing_mp3_bases[@]} -eq 0 ]; then
    echo "无"
else
    for base in "${!existing_mp3_bases[@]}"; do
        echo "- $base"
    done
fi
echo "------------------------------------------------------------------"

# 4. 找出所有潜在的 FLAC 专辑目录 (进一步改进识别逻辑)
potential_flac_dirs=()
for dir in "${all_dirs[@]}"; do
    # 优先级1: 优先匹配 [24B-FLAC] 或 [16B-FLAC]
    if [[ "$dir" == *"[24B-FLAC]"* || "$dir" == *"[16B-FLAC]"* ]]; then
        potential_flac_dirs+=("$dir")
    # 优先级2: 匹配包含 "kHz" 且不包含 "MP3" 的目录 (通常为无损高码率)
    elif [[ "${dir^^}" =~ KHZ ]] && ! [[ "${dir^^}" =~ MP3 ]]; then
        potential_flac_dirs+=("$dir")
    # 优先级3: 通用 FLAC 识别：包含 "FLAC" (不区分大小写) 且不包含 "MP3" (不区分大小写)
    elif [[ "${dir^^}" =~ FLAC ]] && ! [[ "${dir^^}" =~ MP3 ]]; then
        potential_flac_dirs+=("$dir")
    fi
done

echo "--- 调试信息：已识别的潜在 FLAC 专辑目录 ---"
if [ ${#potential_flac_dirs[@]} -eq 0 ]; then
    echo "无"
else
    printf '%s\n' "${potential_flac_dirs[@]}"
fi
echo "---------------------------------------------"

# 5. 过滤掉已经存在 MP3-V0 版本的 FLAC 专辑，并处理 FLAC 版本的优先级
declare -A album_candidates_map # 用于存储最终待转换的 FLAC 专辑，键为基础名，值为完整的目录名
for flac_dir in "${potential_flac_dirs[@]}"; do
    flac_base_name=$(get_base_name "$flac_dir")

    # 如果不是强制转换模式，则检查是否已存在 MP3-V0 版本
    if [ "$FORCE_RECONVERT" != "FORCE_RECONVERT" ]; then
        if [[ -n "${existing_mp3_bases["$flac_base_name"]}" ]]; then
            echo "  - 跳过: '$flac_dir' (已存在 MP3-V0 版本或其基础名 '$flac_base_name' 已有 MP3-V0 对应)"
            continue # 跳过此 FLAC 专辑
        fi
    fi

    # 处理 FLAC 专辑的优先级 (例如，优先保留 24B 版本)
    # 如果 album_candidates_map 中已经有这个基础名的专辑
    if [[ -n "${album_candidates_map["$flac_base_name"]}" ]]; then
        current_candidate="${album_candidates_map["$flac_base_name"]}"
        # 如果当前 FLAC 专辑是 24B-FLAC 或 24B-XXkHz，且已有的不是 24B 的，则替换
        if ( [[ "$flac_dir" == *"[24B-"* ]] || [[ "$flac_dir" == *"[24B-"* ]] ) && \
           ! ( [[ "$current_candidate" == *"[24B-"* ]] || [[ "$current_candidate" == *"[24B-"* ]] ); then
            echo "  - 优化: 为基础名 '$flac_base_name' 选择了更高质量的 FLAC 版本: '$flac_dir' (原为 '$current_candidate')"
            album_candidates_map["$flac_base_name"]="$flac_dir"
        fi
    else
        # 第一次遇到这个基础名的 FLAC 专辑
        album_candidates_map["$flac_base_name"]="$flac_dir"
        echo "  - 选中: 添加 FLAC 专辑到待转换列表: '$flac_dir'"
    fi
done

# 6. 组装最终待处理的专辑目录列表
album_candidates=()
for base_name in "${!album_candidates_map[@]}"; do
    album_candidates+=("${album_candidates_map["$base_name"]}")
done

# 按字母顺序排序，使处理顺序更可预测
IFS=$'\n' sorted_album_candidates=($(sort <<<"${album_candidates[*]}"))
unset IFS

# 如果没有待处理的专辑
if [ ${#sorted_album_candidates[@]} -eq 0 ]; then
    echo "没有找到需要转换的 FLAC 专辑。"
    if [ "$FORCE_RECONVERT" != "FORCE_RECONVERT" ]; then
        echo "这可能是因为所有 FLAC 专辑都已存在对应的 MP3-V0 版本，或者命名不符合识别规则。"
        echo "如需强制重新转换，请在脚本后添加 'FORCE_RECONVERT' 参数。"
    fi
    echo "所有符合条件的专辑处理完毕。"
    echo "---------------------------------------------------"
    exit 0
fi

# 7. 遍历每个待处理专辑目录，调用 convert_music.sh 进行转码
echo "---------------------------------------------------"
echo "以下专辑将被处理 (共 ${#sorted_album_candidates[@]} 个):"
printf '  - %s\n' "${sorted_album_candidates[@]}"
echo "---------------------------------------------------"

for SOURCE_TO_CONVERT in "${sorted_album_candidates[@]}"; do
    SOURCE_PATH="$WORKDIR/$SOURCE_TO_CONVERT"
    echo "  - 正在转换专辑: $SOURCE_PATH"
    "$CONVERT_SCRIPT" "$SOURCE_PATH"

    if [ $? -eq 0 ]; then
        echo "  - 专辑 '$SOURCE_TO_CONVERT' 转换成功。"
    else
        echo "  - 专辑 '$SOURCE_TO_CONVERT' 转换失败，请检查上面的错误信息。"
    fi
    echo "---" # 每个专辑处理后的分隔符
done

echo "所有符合条件的专辑处理完毕。"
echo "---------------------------------------------------"