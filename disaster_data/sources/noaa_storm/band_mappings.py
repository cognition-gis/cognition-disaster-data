
# NOAA/NGS has used various versions of the Trimble DSS throughout the program
# Trimble doesn't publicly release band specifications for their DSS sensors
# Aerial band specifications generally don't change much over time (if they do they get narrower)
# Most accurate band specifications I can find are from an old NGS report on Hurricane Sandy
# http://sandy.ccom.unh.edu/publications/library/2014-04-25_Q2FY14_ProgressReport.pdf
DSS = [
        {
            "name": "B01",
            "common_name": "blue",
            "center_wavelength": 455,
            "full_width_half_max": 65
        },
        {
            "name": "B02",
            "common_name": "green",
            "center_wavelength": 540,
            "full_width_half_max": 80
        },
        {
            "name": "B03",
            "common_name": "red",
            "center_wavelength": 640,
            "full_width_half_max": 60
        }
    ]