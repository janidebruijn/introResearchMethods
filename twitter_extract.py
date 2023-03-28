import gzip
import json
from collections import defaultdict
import os
import time
import random
import binascii


def pick_files():
    '''Creates the paths to the files and returns them.'''
    files = []
    count_year_dict = {}
    total_files = 0

    for year in range(2013, 2023):
        for month in range(1, 13):
            if month < 10:
                month = '0' + str(month)
            for day in range(1, 32):
                if day < 10:
                    day = '0' + str(day)
                for hour in [random.randrange(0, 24)]:
                    if hour < 10:
                        hour = '0' + str(hour)
                    file = str(f'/../net/corpora/twitter2/Tweets/{year}/'
                               + f'{month}/{year}{month}{day}:{hour}.out.gz')
                    if os.path.isfile(file):
                        files.append(file)
                        total_files += 1
                        if str(year) in count_year_dict:
                            count_year_dict[str(year)] += 1
                        else:
                            count_year_dict[str(year)] = 1

    return files, count_year_dict, total_files


def print_output(politeness_dict, count_year_dict, total_tweets, total_polite,
                 total_rude):
    '''Prints the output to the terminal and to a .txt file.'''
    for key, value in politeness_dict.items():
        print(f'{key} {(round((value/count_year_dict[key]) * 100, 3))}%')
    print(f'{total_tweets} total tweets\n{total_polite} total polite\n'
          + f'{total_rude} total rude')

    with open('output.txt', 'a') as f:
        print(f'{count_year_dict}', file=f)
        for key, value in politeness_dict.items():
            print(f'{key} {(round((value/count_year_dict[key]) * 100, 3))}%',
                  file=f)
        print(f'{total_tweets} total tweets\n{total_polite} total polite\n'
              + f'{total_rude} total rude', file=f)


def get_markers():
    '''Creates the marker lists and returns them.'''
    politeness_markers = [
        "gelieve", "het spijt me", "graag", "hartelijk", "dank", "alsjeblieft",
        "excuseer", "sorry", "pardon", "alstublieft", "excuses", "mag ik",
        " u ", "met alle respect", "geachte", "meneer", "mevrouw", "heer",
        "dame", "goedemorgen", "goedemiddag", "goedenavond", "respecteer"
    ]

    rudeness_markers = [
        "*", "rot op", "domme", "tering", "idioot", "sukkel", "flikker ",
        "stommerik", "eikel", "lul", "kut", "trut", "klootzak", "debiel",
        "zeikerd", "fuck", "fock", "kanker", "bitch", "hoer ",
        "hoeren", "slet", "tyfus", "tiefes", "tiefus", "tyfes", "shit"
    ]

    return politeness_markers, rudeness_markers


def extract(files, count_year_dict, total_files):
    '''Extracts the wanted data from the tweets in all given files.'''
    politeness_markers, rudeness_markers = get_markers()

    politeness_dict = defaultdict(float)
    total_tweets = 0
    total_polite = 0
    total_rude = 0
    current_files = 0

    print(f'{count_year_dict}\n')

    start = time.perf_counter()

    for file in files:
        current_files += 1
        with gzip.open(file, 'rt', encoding='latin1') as inp:
            n_polite = 0
            n_tweets = 0

            for line in inp:
                try:
                    if "extended_tweet" and '"full_text":' in line:
                        n_tweets += 1
                        total_tweets += 1
                        try:
                            tweet = json.loads(line)['extended_tweet']\
                                    ['full_text'].encode('utf-8')
                        except json.decoder.JSONDecodeError:
                            continue
                    elif '"text":' in line:
                        n_tweets += 1
                        total_tweets += 1
                        try:
                            tweet = json.loads(line)['text'].encode('utf-8')
                        except json.decoder.JSONDecodeError:
                            continue

                    try:
                        tweet = tweet.decode('utf-8').lower()
                    except UnicodeDecodeError:
                        tweet = binascii.b2a_hex(tweet).decode('utf-8').lower()

                    for marker in politeness_markers:
                        if marker in tweet:
                            n_polite += 1
                            total_polite += 1
                            for r_marker in rudeness_markers:
                                if r_marker in tweet:
                                    n_polite -= 1
                                    total_polite -= 1
                                    total_rude += 1
                                    break
                            break
                except KeyError:
                    continue

            year = str(file[-18:-14])
            ratio = n_polite/n_tweets
            politeness_dict[year] += ratio

        print(f'Files checked: {current_files} / {total_files} - '
              + f'{round((current_files/total_files) * 100, 2)}% - '
              + f'{round(time.perf_counter() - start, 1)} seconds elapsed')

    print_output(politeness_dict, count_year_dict, total_tweets, total_polite,
                 total_rude)


def main():
    files, count_year_dict, total_files = pick_files()
    extract(files, count_year_dict, total_files)


if __name__ == '__main__':
    main()
