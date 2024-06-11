from setuptools import setup, find_packages

setup(
    name='srpc', 
    version='0.1.0',  
    author='Rui Jaulino',
    author_email='rui_jaulino@protonmail.com',
    description='Simple RPC framework in Python',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/ruijaulibo/srpc',  # URL to your package's repository
    packages=find_packages(),  # Automatically find your packages
    install_requires=[
        'numpy>=1.23.1', 
        'pandas>=1.3.5', 
    ],
    classifiers=[
        'Programming Language :: Python :: 3',  
        'License :: OSI Approved :: MIT License',  # The license that your package is released under
        'Operating System :: OS Independent',  
    ],
    python_requires='>=3.8',  # Minimum version requirement of Python for your package
)