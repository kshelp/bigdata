from pyarrow import fs  # pip install pyarrow

hdfs = fs.HadoopFileSystem(host='localhost', port=9000, user='ubuntu')

# 1. 디렉토리 목록 조회
info = hdfs.get_file_info(fs.FileSelector('/user/ubuntu/', recursive=False))
for entry in info:
    print(f"{entry.path} - {entry.type.name}")

# 2. HDFS 파일 쓰기
data = "PyArrow로 쓰기 테스트 중입니다.\n안녕하세요 HDFS!"
with hdfs.open_output_stream('/user/ubuntu/data.txt') as f:
    f.write(data.encode('utf-8'))

# 3. 파일 존재 여부 확인
info = hdfs.get_file_info('/user/ubuntu/data.txt')
if info.type != fs.FileType.NotFound:
    print("파일 존재함")
else:
    print("파일 없음")

# 4. 디렉터리 생성
hdfs.create_dir('/user/ubuntu/new_dir', recursive=True)

# 5. 파일/디렉토리 삭제
# 파일 삭제
#hdfs.delete_file('/user/ubuntu/test_output.txt')
# 디렉터리 삭제
hdfs.delete_dir('/user/ubuntu/new_dir')

# 6. 파일 복사 (로컬 <-> HDFS)
# 로컬 파일을 HDFS로 업로드
with open('3-3-2.py', 'rb') as local_f, \
     hdfs.open_output_stream('/user/ubuntu/3-3-2.py') as hdfs_f:
    hdfs_f.write(local_f.read())

# 7. HDFS 파일을 로컬로 다운로드
with hdfs.open_input_file('/user/ubuntu/data.txt') as hdfs_f, \
     open('data.txt', 'wb') as local_f:
    local_f.write(hdfs_f.read())
