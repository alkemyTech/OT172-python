def logger(relative_path):
    import logging
    """Function to configure the code logs

    Args: relativ path to .log file"""
    logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d',
                        filename=f'{path}/{relative_path}', encoding='utf-8', level=logging.ERROR)
    logging.debug("")
    logging.info("")
    logging.warning("")
    logging.critical("")
    return None