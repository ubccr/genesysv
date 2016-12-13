def generate_url(full_path)
    import os, time, hashlib
    secret = "$f^NSw8KKf#VzjF2F8*heyzkeu+Dg$qY" # Same as AuthTokenSecret
    protectedPath = "/bam_files/" # Same as AuthTokenPrefix
    ipLimitation = False # Same as AuthTokenLimitByIp
    hexTime = "{0:x}".format(int(time.time())) # Time in Hexadecimal

    if ipLimitation:
        token = hashlib.md5(''.join([secret, full_path, hexTime, os.environ["REMOTE_ADDR"]])).hexdigest()
    else:
        token = hashlib.md5(''.join([secret, full_path, hexTime]).encode('utf-8')).hexdigest()

    url = ''.join([protectedPath, token, "/", hexTime, full_path])

    return url
