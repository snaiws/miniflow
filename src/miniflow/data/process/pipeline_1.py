from .base import Pipeline
from .loaninfo.preprocess import split_loaninfo_1, encoding_loaninfo_1, typing_loaninfo_1, get_max_values, scaler_loaninfo_1


class PreprocessPipeline(Pipeline):
    def __call__(self, train, val, test):
        X_train, y_train = split_loaninfo_1(train)
        X_val, y_val = split_loaninfo_1(val)
        X_test, y_test = split_loaninfo_1(test)

        X_train = typing_loaninfo_1(X_train)
        X_val = typing_loaninfo_1(X_val)
        X_test = typing_loaninfo_1(X_test)

        max_values = get_max_values(X_train)

        X_train = scaler_loaninfo_1(X_train, max_values)
        X_val = scaler_loaninfo_1(X_val, max_values)
        X_test = scaler_loaninfo_1(X_test, max_values)
        
        return X_train, y_train, X_val, y_val, X_test, y_test