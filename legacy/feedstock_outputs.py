import os
import json
from sqlalchemy import create_engine, Column, Integer, String, Table, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import tqdm
from sqlalchemy.exc import IntegrityError


Base = declarative_base()

class Prefix(Base):
    __tablename__ = 'prefix'
    id = Column(Integer, primary_key=True)
    prefix = Column(String)
    children = relationship('Prefix', cascade='all,delete', back_populates='parent')
    parent_id = Column(Integer, ForeignKey('prefix.id'))
    files = relationship('File', back_populates='prefix')

    parent = relationship('Prefix', remote_side=[id], back_populates='children')

    def __repr__(self):
        return f'<Prefix(prefix={self.prefix}, id={self.id})>'

class File(Base):
    __tablename__ = 'file'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    feedstocks = relationship('Feedstock', secondary='file_feedstock')
    prefix_id = Column(Integer, ForeignKey('prefix.id'))
    prefix = relationship('Prefix', back_populates='files')

    def __repr__(self):
        return f'<File(name={self.name}, id={self.id})>'

class Feedstock(Base):
    __tablename__ = 'feedstock'
    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return f'<Feedstock(name={self.name}, id={self.id})>'

file_feedstock = Table('file_feedstock', Base.metadata,
    Column('file_id', Integer, ForeignKey('file.id')),
    Column('feedstock_id', Integer, ForeignKey('feedstock.id'))
)

# def traverse_directory(directory):
#     engine = create_engine('sqlite:///example.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)
#     session = Session()

#     prefix_stack = [(session.query(Prefix).filter_by(prefix='').first(), directory)]
#     while prefix_stack:
#         prefix, directory = prefix_stack.pop()
#         paths = list(os.scandir(directory)) # Convert scandir iterator to a list
#         with tqdm.tqdm(total=len(paths), desc=f'Processing files in {directory}') as progress:
#             for entry in paths:
#                 if entry.is_dir():
#                     child_prefix = session.query(Prefix).filter_by(prefix=entry.name, parent=prefix).first()
#                     if child_prefix is None:
#                         child_prefix = Prefix(prefix=entry.name, parent=prefix)
#                         session.add(child_prefix)
#                         session.commit()
#                     prefix_stack.append((child_prefix, entry.path))
#                 elif entry.is_file() and entry.name.endswith('.json'):
#                     with open(entry.path) as f:
#                         data = json.load(f)
#                         feedstocks = data['feedstocks']
#                         file = session.query(File).filter_by(name=entry.name, prefix=prefix).first()
#                         if file is None:
#                             file = File(name=entry.name, prefix=prefix)
#                             session.add(file)
#                             session.commit()
#                         for feedstock_name in feedstocks:
#                             feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
#                             if feedstock is None:
#                                 feedstock = Feedstock(name=feedstock_name)
#                                 session.add(feedstock)
#                                 session.commit()
#                             file.feedstocks.append(feedstock)
#                 progress.update()

#     session.close()


# def traverse_directory(directory):
#     engine = create_engine('sqlite:///example2.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)
#     session = Session()

#     prefix_stack = [(session.query(Prefix).filter_by(prefix='').first(), directory)]
#     while prefix_stack:
#         prefix, directory = prefix_stack.pop()
#         paths = [entry for entry in os.scandir(directory)]
#         with tqdm.tqdm(total=len(paths), desc=f'Processing files in {directory}') as progress:
#             for entry in paths:
#                 if entry.is_dir():
#                     child_prefix = session.query(Prefix).filter_by(prefix=entry.name, parent=prefix).first()
#                     if child_prefix is None:
#                         child_prefix = Prefix(prefix=entry.name, parent=prefix)
#                         session.add(child_prefix)
#                         session.commit()
#                     prefix_stack.append((child_prefix, entry.path))
#                 elif entry.is_file() and entry.name.endswith('.json'):
#                     with open(entry.path) as f:
#                         data = json.load(f)
#                         feedstocks = data['feedstocks']
#                         file = session.query(File).filter_by(name=entry.name, prefix=prefix).first()
#                         if file is None:
#                             file = File(name=entry.name, prefix=prefix)
#                             session.add(file)
#                             session.commit()
#                         for feedstock_name in feedstocks:
#                             feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
#                             if feedstock is None:
#                                 feedstock = Feedstock(name=feedstock_name)
#                                 session.add(feedstock)
#                                 session.commit()
#                             file.feedstocks.append(feedstock)
#                 progress.update()

#     session.close()


# from contextlib import contextmanager

# @contextmanager
# def session_scope(Session):
#     session = Session()
#     try:
#         yield session
#         session.commit()
#     except:
#         session.rollback()
#         raise
#     finally:
#         session.close()

# def traverse_directory(directory):
#     engine = create_engine('sqlite:///example3.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)

#     prefix_stack = [(Session().query(Prefix).filter_by(prefix='').first(), directory)]
#     while prefix_stack:
#         prefix, sub_directory = prefix_stack.pop()
#         paths = os.scandir(sub_directory)
#         with tqdm.tqdm(total=len(list(paths)), desc=f'Processing files in {sub_directory}') as progress, session_scope(Session) as session:
#             print(f'Processing files in {sub_directory}')
#             for entry in paths:
#                 if entry.is_dir():
#                     child_prefix = session.query(Prefix).filter_by(prefix=entry.name, parent=prefix).first()
#                     if child_prefix is None:
#                         child_prefix = Prefix(prefix=entry.name, parent=prefix)
#                         session.add(child_prefix)
#                     prefix_stack.append((child_prefix, entry.path))
#                 elif entry.is_file() and entry.name.endswith('.json'):
#                     with open(entry.path) as f:
#                         data = json.load(f)
#                         feedstocks = data['feedstocks']
#                         file = session.query(File).filter_by(name=entry.name, prefix=prefix).first()
#                         if file is None:
#                             file = File(name=entry.name, prefix=prefix)
#                             session.add(file)
#                         for feedstock_name in feedstocks:
#                             feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
#                             if feedstock is None:
#                                 feedstock = Feedstock(name=feedstock_name)
#                                 session.add(feedstock)
#                             file.feedstocks.append(feedstock)
#                 progress.update()

import pathlib

def get_relative_path(base_path, target_path):
    if base_path == target_path:
        return '/'
    else:
        base_path = pathlib.Path(base_path)
        target_path = pathlib.Path(target_path)
        return target_path.relative_to(base_path)


# def traverse_directory(directory):
#     engine = create_engine('sqlite:///example3.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)
#     session = Session()

#     prefix_stack = [(session.query(Prefix).filter_by(prefix='').first(), directory)]
#     while prefix_stack:
#         prefix, sub_directory = prefix_stack.pop()
#         paths = [entry for entry in os.scandir(sub_directory)]
#         with tqdm.tqdm(total=len(paths), desc=f'Processing files in {get_relative_path(directory, sub_directory)}') as progress:
#             for entry in paths:
#                 if entry.is_dir():
#                     child_prefix = session.query(Prefix).filter_by(prefix=entry.name, parent=prefix).first()
#                     if child_prefix is None:
#                         child_prefix = Prefix(prefix=entry.name, parent=prefix)
#                         session.add(child_prefix)
#                         session.commit()
#                     prefix_stack.append((child_prefix, entry.path))
#                 elif entry.is_file() and entry.name.endswith('.json'):
#                     with open(entry.path) as f:
#                         data = json.load(f)
#                         feedstocks = data['feedstocks']
#                         file = session.query(File).filter_by(name=entry.name, prefix=prefix).first()
#                         if file is None:
#                             file = File(name=entry.name, prefix=prefix)
#                             session.add(file)
#                             session.commit()
#                         for feedstock_name in feedstocks:
#                             feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
#                             if feedstock is None:
#                                 feedstock = Feedstock(name=feedstock_name)
#                                 session.add(feedstock)
#                                 session.commit()
#                             file.feedstocks.append(feedstock)
#                 progress.update()

#     session.close()


# def traverse_directory(directory):
#     engine = create_engine('sqlite:///example4.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)
#     session = Session()

#     # Collect a list of all files to process
#     files_to_process = []
#     prefix_stack = [(session.query(Prefix).filter_by(prefix='').first(), directory)]
#     while prefix_stack:
#         prefix, sub_directory = prefix_stack.pop()
#         paths = [entry for entry in os.scandir(sub_directory)]
#         for entry in paths:
#             if entry.is_dir():
#                 child_prefix = session.query(Prefix).filter_by(prefix=entry.name, parent=prefix).first()
#                 if child_prefix is None:
#                     child_prefix = Prefix(prefix=entry.name, parent=prefix)
#                     session.add(child_prefix)
#                     session.commit()
#                 prefix_stack.append((child_prefix, entry.path))
#             elif entry.is_file() and entry.name.endswith('.json'):
#                 files_to_process.append((entry.path, prefix))

#     # Process each file and update the progress bar
#     with tqdm.tqdm(total=len(files_to_process)) as progress:
#         for file_path, prefix in files_to_process:
#             with open(file_path) as f:
#                 data = json.load(f)
#                 feedstocks = data['feedstocks']
#                 file = session.query(File).filter_by(name=os.path.basename(file_path), prefix=prefix).first()
#                 if file is None:
#                     file = File(name=os.path.basename(file_path), prefix=prefix)
#                     session.add(file)
#                     session.commit()
#                 for feedstock_name in feedstocks:
#                     feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
#                     if feedstock is None:
#                         feedstock = Feedstock(name=feedstock_name)
#                         session.add(feedstock)
#                         session.commit()
#                     file.feedstocks.append(feedstock)
#             progress.update()

#     session.close()

# import multiprocessing
# from multiprocessing import Pool


# def worker(args):
#     file_path, prefix = args
#     with open(file_path) as f:
#         data = json.load(f)
#         feedstocks = data['feedstocks']
#         with engine.connect() as conn:
#             file = conn.execute(
#                 session.query(File).filter_by(name=os.path.basename(file_path), prefix=prefix).statement
#             ).first()
#             if file is None:
#                 file = File(name=os.path.basename(file_path), prefix=prefix)
#                 session.add(file)
#             for feedstock_name in feedstocks:
#                 feedstock = conn.execute(
#                     session.query(Feedstock).filter_by(name=feedstock_name).statement
#                 ).first()
#                 if feedstock is None:
#                     feedstock = Feedstock(name=feedstock_name)
#                     session.add(feedstock)
#                 file.feedstocks.append(feedstock)
#             session.flush()
#         return True



# def traverse_directory(directory):
#     global engine, session
#     engine = create_engine('sqlite:///example_multi.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)
#     session = Session()

#     # Collect a list of all files to process
#     files_to_process = []
#     prefix_stack = [(session.query(Prefix).filter_by(prefix='').first(), directory)]
#     while prefix_stack:
#         prefix, sub_directory = prefix_stack.pop()
#         paths = [entry for entry in os.scandir(sub_directory)]
#         for entry in paths:
#             if entry.is_dir():
#                 child_prefix = session.query(Prefix).filter_by(prefix=entry.name, parent=prefix).first()
#                 if child_prefix is None:
#                     child_prefix = Prefix(prefix=entry.name, parent=prefix)
#                     session.add(child_prefix)
#                     session.commit()
#                 prefix_stack.append((child_prefix, entry.path))
#             elif entry.is_file() and entry.name.endswith('.json'):
#                 files_to_process.append((entry.path, prefix))

#     # Process each file and update the progress bar
#     with tqdm.tqdm(total=len(files_to_process)) as progress:
#         with Pool() as pool:
#             for result in pool.imap_unordered(worker, files_to_process):
#                 progress.update()

#     session.close()

def traverse_directory(directory):
    engine = create_engine('sqlite:///example3.db')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Collect a list of all files to process
    prefix_stack = [(session.query(Prefix).filter_by(prefix='').first(), directory)]
    files_processed = 0
    for prefix, sub_directory in prefix_stack:
        paths = [entry for entry in os.scandir(sub_directory)]
        with tqdm.tqdm(total=len(paths), desc=f'Processing files in {sub_directory}') as progress:
            for entry in paths:
                if entry.is_dir():
                    child_prefix = session.query(Prefix).filter_by(prefix=entry.name, parent=prefix).first()
                    if child_prefix is None:
                        child_prefix = Prefix(prefix=entry.name, parent=prefix)
                        session.add(child_prefix)
                        session.commit()
                    prefix_stack.append((child_prefix, entry.path))
                elif entry.is_file() and entry.name.endswith('.json'):
                    files_processed += 1
                    with open(entry.path) as f:
                        data = json.load(f)
                        feedstocks = data['feedstocks']
                        file = session.query(File).filter_by(name=entry.name, prefix=prefix).first()
                        if file is None:
                            file = File(name=entry.name, prefix=prefix)
                            session.add(file)
                            session.commit()
                        for feedstock_name in feedstocks:
                            feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
                            if feedstock is None:
                                feedstock = Feedstock(name=feedstock_name)
                                session.add(feedstock)
                                session.commit()
                            file.feedstocks.append(feedstock)
                progress.update()
    session.close()
    # tqdm.tqdm.write(f'{files_processed} files processed')


# def traverse_directory(directory):
#     engine = create_engine('sqlite:///example4.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)

#     full_dir_paths = [(root, dirs, files) for root, dirs, files in os.walk(directory)]

#     with Session() as session:
#         files_processed = 0
#         with tqdm.tqdm(total=len(full_dir_paths)) as progress:
#             for root, dirs, files in full_dir_paths:
#                 prefix = session.query(Prefix).filter_by(prefix='', parent=None).first()
#                 for dir_name in root.split(os.path.sep)[1:]:
#                     prefix = session.query(Prefix).filter_by(prefix=dir_name, parent=prefix).first()
#                     if prefix is None:
#                         prefix = Prefix(prefix=dir_name, parent=prefix)
#                         session.add(prefix)

#                 for file_name in files:
#                     if file_name.endswith('.json'):
#                         files_processed += 1
#                         file_path = os.path.join(root, file_name)
#                         with open(file_path) as f:
#                             data = json.load(f)
#                             feedstocks = data['feedstocks']
#                             file = session.query(File).filter_by(name=file_name, prefix=prefix).first()
#                             if file is None:
#                                 file = File(name=file_name, prefix=prefix)
#                                 session.add(file)
#                             for feedstock_name in feedstocks:
#                                 feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
#                                 if feedstock is None:
#                                     feedstock = Feedstock(name=feedstock_name)
#                                     session.add(feedstock)
#                                 file.feedstocks.append(feedstock)
#                 progress.update()
#         session.commit()

# def traverse_directory(directory):
#     engine = create_engine('sqlite:///example5.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)

#     full_dir_paths = [(root, dirs, files) for root, dirs, files in os.walk(directory)]

#     with Session() as session:
#         files_processed = 0
#         with tqdm.tqdm(total=len(full_dir_paths)) as progress:
#             for root, dirs, files in full_dir_paths:
#                 # Get the postfix directory path
#                 # postfix_dir = os.path.join(*(root.split(os.path.sep)[root.split(os.path.sep).index(directory.split(os.path.sep)[0]):]))
#                 postfix_dir = str(pathlib.Path(root).relative_to(directory))

#                 prefix = session.query(Prefix).filter_by(prefix='', parent=None).first()
#                 for dir_name in postfix_dir.split(os.path.sep)[1:]:
#                     prefix = session.query(Prefix).filter_by(prefix=dir_name, parent=prefix).first()
#                     if prefix is None:
#                         prefix = Prefix(prefix=dir_name, parent=prefix)
#                         session.add(prefix)

#                 for file_name in files:
#                     if file_name.endswith('.json'):
#                         files_processed += 1
#                         file_path = os.path.join(root, file_name)
#                         with open(file_path) as f:
#                             data = json.load(f)
#                             feedstocks = data['feedstocks']
#                             file = session.query(File).filter_by(name=file_name, prefix=prefix).first()
#                             if file is None:
#                                 file = File(name=file_name, prefix=prefix)
#                                 session.add(file)
#                             for feedstock_name in feedstocks:
#                                 feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
#                                 if feedstock is None:
#                                     feedstock = Feedstock(name=feedstock_name)
#                                     session.add(feedstock)
#                                 file.feedstocks.append(feedstock)
#                 progress.update()
#         session.commit()

# def traverse_directory(directory):
#     engine = create_engine('sqlite:///example6.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)

#     full_dir_paths = [(root, dirs, files) for root, dirs, files in os.walk(directory)]

#     with Session() as session:
#         files_processed = 0
#         with tqdm.tqdm(total=len(full_dir_paths)) as progress:
#             for root, dirs, files in full_dir_paths:
#                 # Get the postfix directory path
#                 postfix_dir = str(pathlib.Path(root).relative_to(directory))

#                 # Find the parent directory
#                 parent_dir = session.query(Prefix).filter_by(prefix='', parent=None).first()
#                 if postfix_dir:
#                     parent_dir_path = os.path.dirname(postfix_dir)
#                     parent_dir = session.query(Prefix).filter_by(prefix=parent_dir_path, parent=None).first()

#                 # Create or retrieve the current directory
#                 current_dir_name = os.path.basename(postfix_dir)
#                 current_dir = session.query(Prefix).filter_by(prefix=current_dir_name, parent=parent_dir).first()
#                 if current_dir is None:
#                     current_dir = Prefix(prefix=current_dir_name, parent=parent_dir)
#                     session.add(current_dir)

#                 # Process the files
#                 for file_name in files:
#                     if file_name.endswith('.json'):
#                         files_processed += 1
#                         file_path = os.path.join(root, file_name)
#                         with open(file_path) as f:
#                             data = json.load(f)
#                             feedstocks = data['feedstocks']
#                             file = session.query(File).filter_by(name=file_name, prefix=current_dir).first()
#                             if file is None:
#                                 file = File(name=file_name, prefix=current_dir)
#                                 session.add(file)
#                             for feedstock_name in feedstocks:
#                                 feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
#                                 if feedstock is None:
#                                     feedstock = Feedstock(name=feedstock_name)
#                                     session.add(feedstock)
#                                 file.feedstocks.append(feedstock)
#                 progress.update()
#         session.commit()

# def traverse_directory(directory):
#     engine = create_engine('sqlite:///example7.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)

#     full_dir_paths = [(root, dirs, files) for root, dirs, files in os.walk(directory)]

#     with Session() as session:
#         files_processed = 0
#         with tqdm.tqdm(total=len(full_dir_paths)) as progress:
#             for root, dirs, files in full_dir_paths:
#                 # Get the postfix directory path
#                 postfix_dir = str(pathlib.Path(root).relative_to(directory))

#                 prefix = session.query(Prefix).filter_by(prefix='', parent=None).first()
#                 for dir_name in postfix_dir.split(os.path.sep)[1:]:
#                     prefix = session.query(Prefix).filter_by(prefix=dir_name, parent=prefix).first()
#                     if prefix is None:
#                         parent = prefix
#                         if dir_name == postfix_dir.split(os.path.sep)[1]:
#                             parent = None
#                         prefix = Prefix(prefix=dir_name, parent=parent)
#                         session.add(prefix)

#                 for file_name in files:
#                     if file_name.endswith('.json'):
#                         files_processed += 1
#                         file_path = os.path.join(root, file_name)
#                         with open(file_path) as f:
#                             data = json.load(f)
#                             feedstocks = data['feedstocks']
#                             file = session.query(File).filter_by(name=file_name, prefix=prefix).first()
#                             if file is None:
#                                 file = File(name=file_name, prefix=prefix)
#                                 session.add(file)
#                             for feedstock_name in feedstocks:
#                                 feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
#                                 if feedstock is None:
#                                     feedstock = Feedstock(name=feedstock_name)
#                                     session.add(feedstock)
#                                 file.feedstocks.append(feedstock)
#                 progress.update()
#         session.commit()

# def traverse_directory(directory):
#     engine = create_engine('sqlite:///example8.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)

#     full_dir_paths = [(root, dirs, files) for root, dirs, files in os.walk(directory)]

#     with Session() as session:
#         files_processed = 0
#         with tqdm.tqdm(total=len(full_dir_paths)) as progress:
#             for root, dirs, files in full_dir_paths:
#                 # Get the postfix directory path
#                 postfix_dir = str(pathlib.Path(root).relative_to(directory))
#                 prefix = session.query(Prefix).filter_by(prefix='', parent=None).first()

#                 if len(postfix_dir.split('/')) == 1:
#                     # This is the root directory
#                     if prefix is None:
#                         prefix = Prefix(prefix='', parent=None)
#                         session.add(prefix)
#                     session.add(prefix)
#                 else:
#                     # This is a subdirectory
#                     for dir_name in postfix_dir.split(os.path.sep)[1:]:
#                         prefix = session.query(Prefix).filter_by(prefix=dir_name, parent=prefix).first()
#                         if prefix is None:
#                             prefix = Prefix(prefix=dir_name, parent=prefix)
#                             session.add(prefix)

#                 for file_name in files:
#                     if file_name.endswith('.json'):
#                         files_processed += 1
#                         file_path = os.path.join(root, file_name)
#                         with open(file_path) as f:
#                             data = json.load(f)
#                             feedstocks = data['feedstocks']
#                             file = session.query(File).filter_by(name=file_name, prefix=prefix).first()
#                             if file is None:
#                                 file = File(name=file_name, prefix=prefix)
#                                 session.add(file)
#                             for feedstock_name in feedstocks:
#                                 feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
#                                 if feedstock is None:
#                                     feedstock = Feedstock(name=feedstock_name)
#                                     session.add(feedstock)
#                                 file.feedstocks.append(feedstock)
#                 progress.update()
#         session.commit()

# def traverse_directory(directory):
#     engine = create_engine('sqlite:///example9.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)

#     full_dir_paths = [(root, dirs, files) for root, dirs, files in os.walk(directory)]

#     with Session() as session:
#         files_processed = 0
#         with tqdm.tqdm(total=len(full_dir_paths)) as progress:
#             parent_prefix = None  # initialize parent_prefix to None
#             for root, dirs, files in full_dir_paths:
#                 # Get the postfix directory path
#                 postfix_dir = str(pathlib.Path(root).relative_to(directory))

#                 prefix = session.query(Prefix).filter_by(prefix='', parent=parent_prefix).first()
#                 for dir_name in postfix_dir.split(os.path.sep)[1:]:
#                     prefix = session.query(Prefix).filter_by(prefix=dir_name, parent=parent_prefix).first()
#                     if prefix is None:
#                         prefix = Prefix(prefix=dir_name, parent=parent_prefix)
#                         session.add(prefix)

#                     # set the parent_prefix for the next level of the hierarchy
#                     parent_prefix = prefix

#                 for file_name in files:
#                     if file_name.endswith('.json'):
#                         files_processed += 1
#                         file_path = os.path.join(root, file_name)
#                         with open(file_path) as f:
#                             data = json.load(f)
#                             feedstocks = data['feedstocks']
#                             file = session.query(File).filter_by(name=file_name, prefix=prefix).first()
#                             if file is None:
#                                 file = File(name=file_name, prefix=prefix)
#                                 session.add(file)
#                             for feedstock_name in feedstocks:
#                                 feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
#                                 if feedstock is None:
#                                     feedstock = Feedstock(name=feedstock_name)
#                                     session.add(feedstock)
#                                 file.feedstocks.append(feedstock)
#                 progress.update()
#         session.commit()

# def traverse_directory(directory):
#     engine = create_engine('sqlite:///example10.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)

#     full_dir_paths = [(root, dirs, files) for root, dirs, files in os.walk(directory)]

#     with Session() as session:
#         files_processed = 0
#         with tqdm.tqdm(total=len(full_dir_paths)) as progress:
#             for root, dirs, files in full_dir_paths:
#                 # Get the postfix directory path
#                 postfix_dir = str(pathlib.Path(root).relative_to(directory))

#                 prefix = session.query(Prefix).filter_by(prefix='', parent=None).first()
#                 parent_id = prefix.id if prefix else None
#                 for dir_name in postfix_dir.split(os.path.sep)[1:]:
#                     prefix = session.query(Prefix).filter_by(prefix=dir_name, parent_id=parent_id).first()
#                     if prefix is None:
#                         prefix = Prefix(prefix=dir_name, parent_id=parent_id)
#                         session.add(prefix)
#                         session.flush() # need to flush to get the prefix's id
#                     parent_id = prefix.id

#                 for file_name in files:
#                     if file_name.endswith('.json'):
#                         files_processed += 1
#                         file_path = os.path.join(root, file_name)
#                         with open(file_path) as f:
#                             data = json.load(f)
#                             feedstocks = data['feedstocks']
#                             file = session.query(File).filter_by(name=file_name, prefix=prefix).first()
#                             if file is None:
#                                 file = File(name=file_name, prefix=prefix)
#                                 session.add(file)
#                             for feedstock_name in feedstocks:
#                                 feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
#                                 if feedstock is None:
#                                     feedstock = Feedstock(name=feedstock_name)
#                                     session.add(feedstock)
#                                 file.feedstocks.append(feedstock)
#                 progress.update()
#         session.commit()


# def traverse_directory(directory):
#     engine = create_engine('sqlite:///example11.db')
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine)

#     full_dir_paths = [(root, dirs, files) for root, dirs, files in os.walk(directory)]

#     with Session() as session:
#         # Create root prefix
#         root_prefix = Prefix(prefix='', parent=None)
#         session.add(root_prefix)
#         session.commit()

#         files_processed = 0
#         with tqdm.tqdm(total=len(full_dir_paths)) as progress:
#             for root, dirs, files in full_dir_paths:
#                 # Get the postfix directory path
#                 postfix_dir = str(pathlib.Path(root).relative_to(directory))

#                 # Create prefix object for each directory in the postfix_dir
#                 parent_prefix = root_prefix
#                 for dir_name in postfix_dir.split(os.path.sep)[1:]:
#                     prefix = session.query(Prefix).filter_by(prefix=dir_name, parent=parent_prefix).first()
#                     if prefix is None:
#                         prefix = Prefix(prefix=dir_name, parent=parent_prefix)
#                         session.add(prefix)
#                     parent_prefix = prefix

#                 # Add files to the database
#                 for file_name in files:
#                     if file_name.endswith('.json'):
#                         files_processed += 1
#                         file_path = os.path.join(root, file_name)
#                         with open(file_path) as f:
#                             data = json.load(f)
#                             feedstocks = data['feedstocks']
#                             file_prefix = session.query(Prefix).filter_by(prefix=postfix_dir.split(os.path.sep)[-1], parent=parent_prefix).first()
#                             file = session.query(File).filter_by(name=file_name, prefix=file_prefix).first()
#                             if file is None:
#                                 file = File(name=file_name, prefix=file_prefix)
#                                 session.add(file)
#                             for feedstock_name in feedstocks:
#                                 feedstock = session.query(Feedstock).filter_by(name=feedstock_name).first()
#                                 if feedstock is None:
#                                     feedstock = Feedstock(name=feedstock_name)
#                                     session.add(feedstock)
#                                 file.feedstocks.append(feedstock)
#                 progress.update()
#         session.commit()


if __name__ == '__main__':
    traverse_directory(pathlib.Path('/home/vcerutti/Conda-Forge') / 'feedstock-outputs' / 'outputs')

    # engine = create_engine('sqlite:///example11.db')
    # Base.metadata.create_all(engine)
    # Session = sessionmaker(bind=engine)

    # with Session() as session:
    #     feedstock_name = "my_feedstock_name"
    #     query = session.query(File.path).join(File.feedstocks).join(Prefix).join(Feedstock).filter(Feedstock.name == feedstock_name)
    #     file_paths = [result.path for result in query.all()]


