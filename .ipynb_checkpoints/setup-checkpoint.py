from setuptools import setup, find_packages

setup(
    name='my_package',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        # List your project's dependencies here
        # e.g., 'requests >= 2.22.0',
    ],
    # Additional metadata about your package
    author='Young Jin Kim',
    author_email='kimyoungjin06@kisti.re.kr',
    description='MariaDB/MySQL Handling for All type DB',
    # More fields as necessary...
)
