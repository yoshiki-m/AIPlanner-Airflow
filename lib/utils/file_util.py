"""ファイル操作系共通モジュール
"""
import codecs

# エンコード種類
ENCODING_UTF_8 = 'utf-8'
ENCODING_UTF_8_SIG = 'utf_8_sig'    #Signature UTF8
ENCODING_SHIFT_JIS = 'shift_jis'

# 改行コード種類
NEWLINE_UNIX = '\n'
NEWLINE_WINDOWS = '\r\n'

# エンコードエラーオプション
ENCODING_ERROR_IGNORE = 'ignore'
ENCODING_ERROR_REPLACE = 'replace'


def encode_file(from_file_path,
                to_file_path,
                from_file_encoding=ENCODING_UTF_8,
                to_file_encoding=ENCODING_SHIFT_JIS,
                to_file_newline=NEWLINE_WINDOWS,
                encoding_error_option=ENCODING_ERROR_REPLACE):
    """ファイルの文字コードを変換して出力する。

    Args:
        from_file_path (str): 変換前ファイルパス
        to_file_path (str): 変換後ファイルパス
        from_file_encoding (str): 変換前ファイルの文字コード Defaults to ENCODING_UTF_8.
        to_file_encoding (str): 変換後ファイルの文字コード Defaults to ENCODING_SHIFT_JIS.
        to_file_newline (str): 変換後ファイルの改行コード. Defaults to NEWLINE_WINDOWS.
        encoding_error_option (str): エンコードエラー発生時、"ignore"指定で無視、
                                     "replace"指定で「?」に置換する。 Defaults to ENCODING_ERROR_REPLACE.
    """
    with codecs.open(from_file_path, 'r', encoding=from_file_encoding) as from_file:
        with open(to_file_path, 'w', encoding=to_file_encoding, newline=to_file_newline) as to_file:
            if to_file_encoding == ENCODING_SHIFT_JIS or to_file_encoding == ENCODING_UTF_8_SIG:
                # 出力ファイルがShift-JIS / utf_8_sig の場合
                for row in from_file:
                    to_file.write(row.encode(to_file_encoding, encoding_error_option).decode(to_file_encoding))
            else:
                # 出力ファイルがUTF-8の場合
                for row in from_file:
                    to_file.write(row)
