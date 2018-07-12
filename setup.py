# coding:utf-8

from setuptools import setup, find_packages

setup(name='etcd3-slim',
      version='0.1.2',
      description='Thin Etcd3 client',
      long_description=open('README.md').read(),
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.6',
          'Topic :: Software Development :: Libraries',
      ],
      keywords='',
      author='Mailgun Technologies Inc.',
      author_email='admin@mailgunhq.com',
      url='https://www.mailgun.com/',
      license='Apache 2',
      packages=find_packages(exclude=['tests']),
      include_package_data=True,
      zip_safe=True,
      tests_require=[
          'nose',
          'coverage',
          'requests'
      ],
      install_requires=[
          'grpcio',
          'protobuf',
          'six'
      ])
