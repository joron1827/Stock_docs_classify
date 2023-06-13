def inference_text_data(spark,data):
    simple_text_df = spark.createDataFrame(data)
    import re
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    # 정규식 패턴을 이용한 데이터 전처리 함수 정의
    def preprocess_text(text):
        cleaned_text = re.sub(r'[^ ㄱ-ㅎ가-힣 ]+', '', text)
        return cleaned_text

    # UDF(사용자 정의 함수) 등록
    preprocess_udf = udf(preprocess_text, StringType())

    # 전처리 함수를 적용하여 새로운 컬럼 생성
    preprocessed_df = simple_text_df.withColumn("title", preprocess_udf("title"))


    file_path = './stopwords.txt'  # 파일 경로 설정

    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        stopword_list = [line.strip() for line in lines]


    from pyspark.sql.functions import *
    from pyspark.sql.types import Row
    from konlpy.tag import Okt


    def extract_nouns(content):
        okt = Okt()
        tokens = okt.morphs(content)
        tokens = [token for token in tokens if token not in stopword_list]
        
        return tokens

    extract_nouns_udf = udf(extract_nouns, ArrayType(StringType()))

    tokenized_df = preprocessed_df.withColumn("tokens", extract_nouns_udf("title"))


    from pyspark.sql.functions import explode, col, array_except, array, lit, udf
    from pyspark.sql.types import ArrayType, StringType

    top_words_list = ['은', '오늘', '는', '다', '도', '만', '주', '원', '한', '개미', '매수', '된', '삭제', '게시', '물의', 'ㅋㅋ', '주식', '고', '답글', '안', '라', '개', '내일', '상', '시', '내', '매도', '만원', '대', '이다']

    filter_words = udf(lambda tokens: [word for word in tokens if word not in top_words_list], ArrayType(StringType()))

    # Add a new column 'filtered_tokens' with only the words present in top_words_list
    filtered_df = tokenized_df.withColumn('filtered_tokens', filter_words(col('tokens')))

    filtered_df = filtered_df.dropDuplicates(["title","tokens"])

    from pyspark.sql.types import BooleanType


    # UDF 정의
    def is_empty_string(value):
        return len(value) == 0

    # UDF 등록
    is_empty_string_udf = udf(is_empty_string, BooleanType())

    # filtered_tokens 컬럼의 빈 문자열을 결측치로 간주하여 제거
    df_filtered = filtered_df.filter(~is_empty_string_udf(col("filtered_tokens")))

    from pyspark.ml.feature import HashingTF, IDF
    from pyspark.ml import PipelineModel


    # 모델 로드
    loaded_model = PipelineModel.load("hdfs://192.168.219.121:9000/crawling_stock_text_merge/hashing_idf_model")


    # 데이터 변환
    transformed_data = loaded_model.transform(df_filtered)


    from pyspark.ml.classification import LogisticRegressionModel

    # 모델 로드
    loaded_lr_model = LogisticRegressionModel.load("hdfs://192.168.219.121:9000/crawling_stock_text_merge/lr_model")

    # 데이터 변환
    new_data = loaded_lr_model.transform(transformed_data)

