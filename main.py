from src.fetcher import fetch
from src.preprocessor import preprocess
from src.uploader import upload


def main():
    upload(preprocess(fetch()))


if __name__ == "__main__":
    main()