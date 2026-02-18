from cryptography.fernet import fernet

# print(cryptography.__version__)

fernet_key = fernet.generate_key()
print(fernet_key.decode())  # this will print fernet key