# utilitylib API Reference

금융 데이터 수집 및 처리를 돕는 `utilitylib`의 사용법을 정리한 문서입니다.
<br> 터미널에 아래 커맨드를 입력해 필요 모듈을 설치하세요. 

```sh
pip install -r utilitylib/requirements.txt
```

---

## Package Overview

`utilitylib`은 다음 모듈을 포함합니다. 

| Module | Purpose |
| --- | --- |
| `.chromedriver` | <b>Chrome Driver를 이용한 웹 데이터 수집을 보조하는 모듈입니다.</b> <br><br>  클라우드 환경에서는 정상적으로 동작하지 않아 스크립트 기반 프로그램 제작에 적합합니다. |
| `.gcshandler` | <b>Google Cloud Service에 저장된 파일의 읽기⋅쓰기를 보조하는 모듈입니다.</b> <br><br>로컬 기능을 지원해 편리하게 테스트 케이스를 다룰 수 있습니다. |

---

## `utilitylib.chromedriver`

### Class `ChromeDriver`

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `headless` | `bool` | `False` | `True`인 경우 크롬 팝업 없이 백그라운드에서 실행됩니다. |
| `timers` | `dict` | ```{"buffer_time": 0.3, "load_time": 10}``` | `buffer_time` : 클릭과 클릭 사이의 전환 속도입니다. 짧을수록 실행이 빨라지지만, 기본값보다 작으면 드라이버가 버벅임에 따라 오류 가능성이 있습니다. 느린 컴퓨터에서는 `0.5`에서 `1.0` 사이를 권장합니다. <br><br> `load_time` : 해당 시간동안 크롬 드라이버가 켜지지 않았을 경우 오류를 반환합니다. |

#### Functions

---

## `utilitylib.gcshandler`

### 주요 GCS(Google Cloud Storage) 명령어 안내

| 동작 | 명령어 |
|-|-|
| **로그인**   | `$ gcloud auth application-default login` |
| **파일 업로드** | `$ gcloud storage cp <local_file_name> gs://<bucket_name>/<blob_name>` |
| **업로드 확인**| `$ gcloud storage ls gs://<bucket_name>/<blob_name>` |
| **다운로드** | `$ gcloud storage cp gs://<bucket_name>/<blob_name> <local_file_name>` |

### Class `GCS`

Google Cloud Storage에 저장된 파일을 손쉽게 읽고 쓸 수 있도록 지원합니다. 데이터 저장과 불러오기를 로컬 또는 클라우드 영역에서 선택적으로 사용할 수 있습니다.

| Parameter     | Type | Description            |
| ------------- | ---- | --------------------- |
| `bucket_name` | `str`  | 사용할 클라우드 버킷명   |

#### Functions

- `save(data, blob_name, local=False)` : 파이썬 딕셔너리를 JSON 파일로 저장합니다.  
    - `data`: 저장할 데이터
    - `blob_name`: 저장할 파일명  
    - `local`: `True`면 로컬, `False`면 클라우드에 저장
    - 반환값 : 성공시 `True`, 실패시 `False`  

- `load(local_file_name, blob_name="", local=False)` : 저장된 JSON 파일을 파이썬 딕셔너리로 불러옵니다.
    - `local_file_name`: 로컬 파일명
    - `blob_name`: 클라우드 파일명 (생략시 `local_file_name` 사용)  
    - `local`: `True`면 로컬, `False`면 클라우드에서 불러옴  
    - 반환값 : 성공시 딕셔너리 데이터, 실패시 False  

> 
