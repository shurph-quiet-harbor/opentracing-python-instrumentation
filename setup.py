from setuptools import setup, find_packages

with open('README.md', 'r') as fh:
    long_description = fh.read()

setup(
    name='opentracing_instrumentation',
    version='2.5.0.dev0',
    author='Yuri Shkuro',
    author_email='ys@uber.com',
    description='Tracing Instrumentation using OpenTracing API '
                '(http://opentracing.io)',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='MIT',
    url='https://github.com/uber-common/opentracing-python-instrumentation',
    keywords=['opentracing'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages=find_packages(exclude=['tests', 'tests.*']),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=[
        'future',
        'wrapt',
        'tornado>=4.1,<5',
        'contextlib2',
        'opentracing>=1.1,<2',
        'six',
    ],
    extras_require={
        'tests': [
            'boto3',
            'botocore',
            'coveralls',
            'doubles',
            'flake8',
            'flake8-quotes',
            'mock',
            'psycopg2-binary',
            'sqlalchemy>=1.2.0',
            'pytest>=3.0.0',
            'pytest-cov',
            'pytest-localserver',
            'pytest-mock',
            'pytest-tornado',
            'basictracer<3.0.0',
            'redis',
            'Sphinx',
            'sphinx_rtd_theme',
        ],

        # Currently, moto doesn't support latest boto3/botocore:
        # https://github.com/spulec/moto/pull/1847
        # https://github.com/spulec/moto/pull/1907
        # You are still able to install moto and test boto3
        # instrumentation with it to avoid running of DynamoDB,
        # but it will downgrade boto3/botocore.
        'tests_moto': [
            'moto',
        ]
    },
)
