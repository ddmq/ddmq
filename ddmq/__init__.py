from .broker import broker
from .message import message


def get_bin_path():
    """
    If the executables pip installs are not in the user's PATH they will have to add it. This function should tell them where it has been installed.
    
    Args:
        None

    Returns:
        a string that is the path to the ddmq executable
    """

    # list all files belonging to ddmq
    import pkg_resources
    import os
    try:
        FileNotFoundError
    except NameError:
        FileNotFoundError = IOError

    try:
        for line in pkg_resources.get_distribution('ddmq').get_metadata('installed-files.txt').split(os.linesep):

            # find the executable 'ddmq's path, relative to the installed module
            if os.path.split(line)[-1] == 'ddmq':

                # print the absolute path of the executabel by first joining it with the path to the module itself
                os.path.abspath(os.path.join(os.path.dirname(__file__), line))
    except (FileNotFoundError, IOError) as e:
        raise IOError("{}: \'{}\'. If ddmq was not installed by pip this will not work.".format(e.strerror, e.filename))
