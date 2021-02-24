# -*- coding: utf-8 -*-

"""Main module."""
from boto3 import session
import pandas as pd
import json
from collections import namedtuple
from datetime import date, datetime
from typing import List
from tqdm import tqdm

Key = namedtuple("Key", ["path", "date"])

session = session.Session()


def create_key(path):
    date = path.split("/")[-1]
    path = path
    return Key(path=path, date=datetime.strptime(date, '%Y-%m-%d-%H-%M-%S'))


class Space:
    def __init__(self, aws_access_key_id, aws_secret_access_key) -> None:
        self.aws_acces_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.client = session.client('s3',
                                     region_name='nyc1',
                                     endpoint_url='https://sfo2.digitaloceanspaces.com',
                                     aws_access_key_id=self.aws_acces_key_id,
                                     aws_secret_access_key=self.aws_secret_access_key)

    def save_posts(self, start_time, end_time, path, is_json=False, select_fields=None):
        raw_posts = self._get_krisa_raw(start_time, end_time)
        print(f"Raw posts: {len(raw_posts)}")

        clean_posts = self._normalize_posts(raw_posts)
        print(f"Clean posts: {len(clean_posts)}")

        if is_json:
            self._save_raw(clean_posts, path)
        else:
            if select_fields is None:
                raise ValueError("Provide fields to include into csv")

            df = self._posts_to_pandas(select_fields, clean_posts)
            print(df.shape)
            self._save_pandas(df, path)

    def _get_krisa_keys_files(self):
        response = self.client.list_objects_v2(
            Bucket="athena-monitoring-store", Prefix="krisa/wallstreetbets/posts/")
        keys = []
        done = False
        while not done:
            posts = response['Contents']
            for post in posts:
                keys.append(post['Key'])
            done = not response["IsTruncated"]
            if not done:
                response = self.client.list_objects_v2(Bucket="athena-monitoring-store",
                                                       Prefix="krisa/wallstreetbets/posts/",
                                                       ContinuationToken=response["NextContinuationToken"])
        return keys

    def _load_keys(self, keys):
        all_rows = []

        with tqdm(total=len(keys)) as pbar:
            for post in keys:
                result = self.client.get_object(
                    Bucket="athena-monitoring-store", Key=post)
                text = result["Body"].read().decode()

                if not text:
                    print("post {0}".format(post))
                    continue

                post_dump = json.loads(text)
                all_rows.append(post_dump)
                pbar.update(1)

        return all_rows

    def _get_krisa_raw(self, start_time, end_time):
        keys = self._get_krisa_keys_files()

        dated_keys = [create_key(key) for key in keys]

        keys_in_interval = [
            key.path for key in
            filter(lambda x: start_time <= x.date < end_time, dated_keys)
        ]

        return self._load_keys(keys_in_interval)

    def _normalize_posts(self, raw_posts):
        clean_post = []

        for post in raw_posts:
            if isinstance(post, list):
                for d in post:
                    clean_post.append(d)
                continue
            clean_post.append(post)

        return clean_post

    def _posts_to_pandas(self, fields: List, clean_posts):
        rows = []
        for post in clean_posts:
            rows.append({f: post[f] for f in fields})

        df = pd.DataFrame()
        df = df.append(rows, ignore_index=True)
        return df

    def _save_raw(self, clean_posts, path):
        with open(path, "w") as fp:
            json.dump(clean_posts, fp)

    def _save_pandas(self, df: pd.DataFrame, path):
        df.to_csv(path, index=False)
