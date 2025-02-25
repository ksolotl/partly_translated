import re
import sys
from bs4 import BeautifulSoup, NavigableString, Tag
import mysql.connector
from translate import Translator
from collections import Counter
from typing import List


class Word:
    word: str
    translation: str
    translate_every_time: bool
    original_not_need: bool


class FBTranslate:
    soup: BeautifulSoup
    connection: mysql.connector.connection.MySQLConnection
    start_line: int
    current_line: int
    known_words: List[Word] = []
    translate_words_count = 5
    translates_per_default = 15
    translates_per = 12

    divine_symbols = ['.', ',', '!', '?', ':', ';', '...']

    def __init__(self, file_path):
        self.write_data = True
        self.file_path = file_path
        self.read_book()
        self.connect_to_db()
        self.init_db()
        self.init_book_info()
        self.translator = Translator(from_lang="ru", to_lang="es", provider="mymemory")


    def connect_to_db(self):
        try:
            self.connection = mysql.connector.connect(
                host='localhost',
                user='root',
                password=None,
                database='translator'
            )
            if self.connection.is_connected():
                print("Connected to MySQL database")

        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def close_connection(self):
        if self.connection.is_connected():
            self.connection.close()
            print("MySQL connection is closed")

    def init_db(self):
        self.connection.cursor().execute('''
        CREATE TABLE IF NOT EXISTS books (
            id INT AUTO_INCREMENT PRIMARY KEY, 
            file_path VARCHAR(255) UNIQUE NOT NULL, 
            start_line INT DEFAULT 0,
            last_edit_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
            ''')
        self.connection.commit()

        self.connection.cursor().execute('''
        CREATE TABLE IF NOT EXISTS words (
            id INT AUTO_INCREMENT PRIMARY KEY, 
            word VARCHAR(255) UNIQUE NOT NULL, 
            `translation` VARCHAR(512), 
            `translate_every_time` BOOLEAN DEFAULT FALSE,
            `original_not_need` BOOLEAN DEFAULT FALSE,
            `cnt` smallint default NULL null
            )
            ''')
        self.connection.commit()

    def init_book_info(self):
        cursor = self.connection.cursor()
        cursor.execute('''INSERT IGNORE INTO books (file_path) VALUES (%s)''', (self.file_path,))
        self.connection.commit()
        cursor.execute('''SELECT start_line FROM books WHERE file_path = %s''', (self.file_path,))
        self.start_line = cursor.fetchall().pop()[0]
        self.current_line = self.start_line
        cursor.execute('''SELECT `word`, `translation`, `translate_every_time`, `original_not_need` FROM words ''')
        known_words_tuples = cursor.fetchall()
        for word_tuple in known_words_tuples:
            word = Word()
            word.word = word_tuple[0]
            word.translation = word_tuple[1]
            word.translate_every_time = word_tuple[2]
            word.original_not_need = word_tuple[3]
            self.known_words.append(word)

        cursor.close()


    def should_be_missed(self, string_line: str):
        if string_line.isdigit():
            return True

        if string_line.isdigit():
            return True

        return False

    def translate_word(self, word):
        translation = self.translator.translate(word)
        if translation.strip().startswith('MYMEMORY WARNING'):
            raise Exception('Translation error')

        return translation

    def read_book(self):
        with open(self.file_path, 'r', encoding='utf-8') as xml:
            self.soup = BeautifulSoup(xml.read(), 'lxml')

    def write_book(self):
        if not self.write_data:
            return
        with open(self.file_path, 'w', encoding='utf-8') as file:
            file.write(str(self.soup))

        self.connection.cursor().execute(
            '''UPDATE books SET start_line = %s WHERE file_path = %s''',
                                     (self.current_line, self.file_path))
        self.connection.commit()

    def lowercase_first_letter(self, s: str) -> str:
        if not s:
            return s

        return s[0].lower() + s[1:]

    def translate_line(self, line):
        def get_translation(old_line: str, translated=None, without_original=False):
            first_up = old_line[0].isupper()
            if not translated:
                translated = self.translate_word(old_line)

            translated = translated.capitalize() if first_up else self.lowercase_first_letter(translated)
            if without_original:
                return translated

            line_break = ''
            if old_line.endswith('\n'):
                line_break = '\n'
                old_line = old_line[:-1]
            if old_line.endswith('.'):
                line_break = '.' + line_break
                old_line = old_line[:-1]

            return translated + '(' + old_line.strip(' \t\n\.\!,') + ') ' + line_break

        words = re.findall(r'\b\w+\b|\s+|[^\w\s]', line, re.UNICODE)

        word_count = 0
        i = 0
        result_line = ''
        if len(words) - 4 <= self.translate_words_count <= len(words):
            if self.should_be_missed(line):
                result_line = line
            else:
                result_line = get_translation(line)
                self.translates_per = self.translates_per_default
                word_count = 0
                return result_line

        while i < len(words) - 1:
            length_of_the_current_cycle = self.translate_words_count
            k = self.translate_words_count - 2
            while k <= self.translate_words_count + 2:
                if i + k < len(words) and words[i + k] in self.divine_symbols:
                    length_of_the_current_cycle = k
                    break
                k += 1

            old_words = words[i]
            for j in range(1, length_of_the_current_cycle):
                if i + j < len(words):
                    old_words += words[i + j]

            if re.match(r'\b\w+\b', old_words):
                if self.should_be_missed(old_words):
                    result_line += old_words

                    i += length_of_the_current_cycle
                    continue

                word_count += 1

                known_word = self.find_word(old_words, True)
                if known_word and known_word.translate_every_time:
                    translated_words = get_translation(old_words, known_word.translation, known_word.original_not_need)
                    result_line += translated_words
                    print(translated_words)

                    i += length_of_the_current_cycle
                    word_count = 0
                    continue
                # else:
                #     known_word = self.find_word(old_words, False)
                #     if known_word and known_word.translate_every_time:
                #         words_at_beginning = re.sub(known_word.word + '.*?$', '', old_words)
                #         all_words = re.findall(r'\b\w+\b|\s+|[^\w\s]', words_at_beginning +  known_word.word, re.UNICODE)
                #
                #         translated_words = get_translation(known_word.word, known_word.translation, known_word.original_not_need)
                #         result_line += words_at_beginning +  translated_words
                #         print(translated_words)
                #
                #         i += len(all_words)
                #         word_count = 0
                #         continue

                if word_count >= self.translates_per and not any(symbol in old_words for symbol in self.divine_symbols):
                    translated_words = get_translation(old_words)
                    result_line += translated_words
                    print(translated_words)

                    i += length_of_the_current_cycle
                    word_count = 0
                    continue

            result_line += words[i]
            i += 1

        if i < len(words):
            result_line += words[i]


        if line == result_line:
            self.translates_per -= 1
        else:
            self.translates_per = self.translates_per_default

        return result_line

    def string_difference(self, str1, str2):
        return ''.join([char for char in str1 if char not in str2])

    def test_book(self):
        paragraphs = self.soup.find_all('p')
        strong_indexes = []
        strong_par = []
        for index, par in enumerate(paragraphs):
            if isinstance(par.string, NavigableString) and 'серой планеты' in par.string:
                print(index)
            # if isinstance(par.string, NavigableString) and par.next.name == 'strong':
            #     strong_indexes.append(index)
            #     strong_par.append(par)
        pass

    def grab_book_statistics(self):
        def clean_phases(phrases):
            # return phrases
            return [s for s in phrases if 'джон' not in s and 'рик' not in s and 'изидор' not in s and 'декорд' not in s and 'рейчел' not in s]

        words = re.findall(r'\b\w+\b', self.soup.text.lower())
        phrases2 = [' '.join(words[i:i + 2]) for i in range(len(words) - 1)]
        phrases2  = clean_phases(phrases2)
        phrases3 = [' '.join(words[i:i + 3]) for i in range(len(words) - 2)]
        phrases2  = clean_phases(phrases3)

        phrase_counts2 = Counter(phrases2)
        phrase_counts3 = Counter(phrases3)

        most_common_phrases2 = phrase_counts2.most_common(200)
        # for word, cnt in most_common_phrases2[57:]:
        # for word, cnt in most_common_phrases2:
        #     translation = self.translate_word(word)
        #     self.connection.cursor().execute(
        #         '''INSERT IGNORE INTO words (word, translation, translate_every_time, original_not_need, cnt) VALUES (%s, %s, True, False, %s)''',
        #         (word, translation, cnt))
        #     self.connection.commit()

        most_common_phrases3 = phrase_counts3.most_common(200)
        for word, cnt in most_common_phrases3:
            translation = self.translate_word(word)
            self.connection.cursor().execute(
                '''INSERT IGNORE INTO words (word, translation, translate_every_time, original_not_need, cnt) VALUES (%s, %s, True, False, %s)''',
                (word, translation, cnt))
            self.connection.commit()



    def translate_book(self):
        while self.current_line < len(self.soup.find_all('p')):
            paragraph = self.soup.find_all('p')[self.current_line]
            # pin = [(index, p) for index, p in enumerate(self.soup.find_all('p')) if '5.4' in p.text]
            paragraph_contents = []
            for content_index, content in enumerate(paragraph.contents):
                if isinstance(content, NavigableString):
                    try:
                        translated_content = self.translate_line(content)
                        if translated_content != content:
                            # parts = re.split(pattern1, translated_content)
                            # for part in parts:
                            #     if re.match(pattern1, part):
                            #         translated_text = re.search(pattern2, part).group(2)
                            #         tag = Tag(name='sub')
                            #         tag.string = translated_text
                            #         paragraph_contents.append(tag)
                            #     else:
                            #         paragraph_contents.append(NavigableString(part))

                            paragraph.contents[content_index].replace_with(NavigableString(translated_content))
                        else:
                            paragraph_contents.append(content)
                    except Exception as e:
                        print(f"Error: {e}")
                        self.write_book()
                        return
                else:
                    paragraph_contents.append(content)

            paragraph.contents = paragraph_contents

            # try:
            #     if isinstance(paragraphs.string, NavigableString):
            #         paragraphs.string = self.translate_line(paragraphs.string)
            # except Exception as e:
            #     print(f"Error: {e}")
            #     self.write_book()
            #     return
            #
            self.current_line += 1

            if (self.current_line - self.start_line)%10 == 0:
                self.write_book()
                pass

        self.write_book()
        pass

    def find_word(self, word_string: str, strict = True):
        word_string = word_string.lower().strip()
        for known_word in self.known_words:
            if strict and known_word.word == word_string:
                return known_word
            elif not strict and known_word.word in word_string:
                return known_word

        return None


if __name__ == "__main__":
    translator = FBTranslate(sys.argv[1])
    translator.translate_book()
    # translator.test_book()
    # translator.grab_book_statistics()
