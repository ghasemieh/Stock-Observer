"""
The configuration service loads a configuration file into a
ConfigParser through which configuration values can be accessed

The ``ACTIVE_PROFILE`` environment variable must be set to
a valid profile because there is no default configuration

A valid profile is one for which a ``stock-observer-{profile}.config`` file
exists in the project root
"""

import os
from configparser import ConfigParser


class __Config:
    __path_to_config = 'stock-observer-{}.config'

    __env_var_name = 'ACTIVE_PROFILE'

    __error_no_active_profile_set = """

        Cannot continue without an active configuration profile. 

        To set an active profile, add the environment variable 

            'ACTIVE_PROFILE'

        A valid profile is one for which a 'stock-observer-{profile}.config file
        exists in the project root
        """

    __error_no_file_matching_active_profile = """

        The active profile is set to '{}' but no file named '{}' exists in '{}'
        """

    @property
    def get(self) -> ConfigParser:
        """
        Gets a ConfigParser
        :return:
        """
        assert self.__env_var_name in os.environ, self.__error_no_active_profile_set

        profile = os.environ[self.__env_var_name]
        profile_config_path = self.__path_to_config.format(profile)

        assert os.path.isfile(profile_config_path), self.__error_no_file_matching_active_profile.format(
            profile, profile_config_path, os.getcwd())

        cp = ConfigParser()
        cp.read(self.__path_to_config.format(profile))
        return cp


__config = __Config()


def get() -> ConfigParser:
    return __config.get
