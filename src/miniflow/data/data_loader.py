import pandas as pd

from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split

from .source import DataSourceCSV

def load_data(data_path: str):
    """CSV 파일로부터 train, test 데이터를 로딩하고, 카테고리형 컬럼을 처리합니다."""
    df = DataSourceCSV(data_path).read()
    df.set_index('UID', inplace=True)
    categorical_cols = ['주거 형태', '현재 직장 근속 연수', '대출 목적', '대출 상환 기간']
    df = pd.get_dummies(df, columns=categorical_cols)
    df = df.astype(float)
    
    target = "채무 불이행 여부"

    # train 데이터셋 분할 (train: 60%, validation: 20%, test: 20%) 이건 원래 따로 샘플링 버전 저장해야 함
    train, val = train_test_split(df, test_size=0.4, random_state=421)
    val, test = train_test_split(val, test_size=0.5, random_state=421)

    X_train, y_train = train.drop(columns=[target]), train[target]
    X_val, y_val = val.drop(columns=[target]), val[target]
    X_test, y_test = test.drop(columns=[target]), test[target]

    scaler = MinMaxScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled   = scaler.transform(X_val)
    X_test_scaled  = scaler.transform(X_test)
    X_train = pd.DataFrame(X_train_scaled, index = X_train.index,columns = X_train.columns)
    X_val = pd.DataFrame(X_val_scaled, index = X_val.index,columns = X_val.columns)
    X_test = pd.DataFrame(X_test_scaled, index = X_test.index,columns = X_test.columns)

    return X_train, y_train, X_val, y_val, X_test, y_test