import pandas as pd

from sklearn.model_selection import train_test_split

def idx_loaninfo_1(data):
    data.set_index('UID', inplace=True)
    return data

def encoding_loaninfo_1(data):
    categorical_cols = ['주거 형태', '현재 직장 근속 연수', '대출 목적', '대출 상환 기간']
    data = pd.get_dummies(data, columns=categorical_cols)
    return data

def typing_loaninfo_1(data):
    data = data.astype(float)
    return data

def sampaling_loaninfo_1(data, ratio:tuple, random_state:int):
    train_size, val_size, test_size = ratio
    assert train_size + val_size + test_size == 1, "데이터셋 분할 비율의 합이 1이 아닙니다."

    # train 데이터셋 분할 (train: 60%, validation: 20%, test: 20%) 이건 원래 따로 샘플링 버전 저장해야 함
    train, val = train_test_split(data, test_size=1-train_size, random_state=random_state)
    val, test = train_test_split(val, test_size=1-train_size-val_size, random_state=random_state)
    return train, val, test

def split_loaninfo_1(data):
    target = "채무 불이행 여부"
    X, y = data.drop(columns=[target]), data[target]
    return X, y


def get_max_values(df)-> dict:
    max_values = {}
    for column in df.columns:
        max_values[column] = df[column].max()
    return max_values


def scaler_loaninfo_1(df, max_values:dict):
    scaled_df = df.copy()
    for column in df.columns:
        if column in max_values and max_values[column] != 0:  # 0으로 나누기 방지
            scaled_df[column] = df[column] / max_values[column]
    return scaled_df