#
# This file is part of dsa_tdb (see https://code.europa.eu/dsa/transparency-database/dsa-tdb).
#
# SPDX-License-Identifier: EUPLv1.2
# Copyright (C) 2024 European Union
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the EUROPEAN UNION PUBLIC LICENCE v. 1.2 as
# published by the European Union.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# EUROPEAN UNION PUBLIC LICENCE v. 1.2 for further details.
#
# You should have received a copy of the EUROPEAN UNION PUBLIC LICENCE v. 1.2.
# along with this program.
#
# If not, see < https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12 >.#
import logging
from collections import OrderedDict
from datetime import date, datetime
from enum import Enum, unique
from itertools import chain
from typing import List, Optional, Union

import numpy as np
import psutil
import pyarrow as pa
from pydantic import BaseModel, Field
from pyspark.conf import SparkConf
from strenum import StrEnum

# The location of the daily files table
DAILY_FILES_TABLE_URL = "https://transparency.dsa.ec.europa.eu/explore-data/download"
"""The url of the daily files table."""

DAILY_FILE_S3_BUCKET = "https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com"

DAILY_FILE_NAME_TEMPLATE = "sor-{platform}-{date}-{version}{extension}"
"""The name pattern of the daily files/sha1 once downloaded."""

DAILY_FILE_NAME_REGEX = r"sor-(?P<platform>[\w-]+)-(?P<date>\d{4}-\d{2}-\d{2})-(?P<version>\w+)(?P<extension>\.\w+)"
"""The name regex of the daily files/sha1 once downloaded."""

DAILY_FILE_URL_TEMPLATE = DAILY_FILE_S3_BUCKET + "/" + DAILY_FILE_NAME_TEMPLATE
"""The url pattern of the daily files/sha1."""

DAILY_FILE_EXTENSION = ".zip"
"""The extension of the daily files checksums."""

DAILY_FILE_CHECKSUM_EXTENSION = ".zip.sha1"
"""The extension of the daily files'sha1 checksums."""

DAILY_CHUNKED_FOLDER_NAME_REGEX = r"sor-(?P<platform>[\w-]+)-(?P<date>\d{4}-\d{2}-\d{2})-(?P<version>\w+)"

DAILY_FILE_DATE_FORMAT = "%Y-%m-%d"
"""The format of the date in the daily files/sha1."""

DAILY_FILES_SUBFOLDER_TEMPLATE = "{platform}___{version}"
"""The subfolder pattern of the daily files/sha1 once downloaded."""

DAILY_FILES_SUBFOLDER_REGEX = r"(?P<platform>[\w-]+)___(?P<version>\w+)"
"""The subfolder regex of the daily files/sha1 once downloaded."""

CHUNKED_FILES_SUBFOLDER_NAME = "daily_dumps_chunked"
"""The subfolder where to save the chunked files."""

CHUNKED_FILE_SUCCESS_NAME = "COMPLETE"
"""The name of the file to save in the chunked folder to signal the completion of the chunking."""

ALL_PLATFORMS_ENTRY_VALUE = "All Platforms"
"""The name of the global option in the dropdown menu"""

ALL_PLATFORMS_PLATFORM_NAME = "global"
"""The name of the fictictious global platform to account for all the platforms"""

# Month of first data available
TDB_START_DATE = datetime(2023, 9, 1)


class TDB_dailyDumpsVersion(StrEnum):
    """Enum of the versions of the daily dumps."""

    full = "full"
    light = "light"


class TDB_chunkFormat(StrEnum):
    """Enum of the formats of the chunks."""

    csv = "csv"
    parquet = "parquet"
    pickle = "pickle"


accepted_chunk_formats = [e.value for e in TDB_chunkFormat]


class PreprocessArguments(BaseModel):
    """The base models to validate the download arguments."""

    dump_files_folder: str = Field(..., description="The folder where to download the daily dumps.")
    version: TDB_dailyDumpsVersion = Field(..., description="The version of the daily dumps to download.")
    platform: str = Field(..., description="The platform to download the daily dumps from.")
    check_sha1: bool = Field(True, description="Check the sha1 of the downloaded daily dumps.")
    force_sha1: bool = Field(False, description="Force the sha1 check of the already downloaded daily dumps.")
    from_date: Optional[date] = Field(
        None, description="The date from which to download the daily dumps, format YYYY-MM-DD."
    )
    to_date: Optional[date] = Field(
        None, description="The date until which to download the daily dumps, format YYYY-MM-DD."
    )
    n_processes: int = Field(1, description="The number of processes to use to download the daily dumps.")
    do_chunking: bool = Field(False, description="Whether to contextually chunk the downloaded daily dumps.")
    platforms_to_exclude: Optional[List[str]] = Field(None, description="The platforms to exclude from the download.")
    chunk_size: int = Field(
        1000000, description="The chunksize to use when contextually chunking the downloaded daily dumps."
    )
    chunk_format: TDB_chunkFormat = Field(TDB_chunkFormat.csv, description="The format of the chunks to save.")
    delete_original: bool = Field(False, description="Whether to delete the original daily dumps after chunking.")
    loglevel: int = Field(logging.INFO, description="The log level to use.")
    override_chunked_subfolder: str = Field(
        CHUNKED_FILES_SUBFOLDER_NAME, description="The subfolder to use for the chunked files."
    )
    raise_on_error: bool = Field(
        True,
        description="Whether to raise when a dump or chunk sha1 check fails. if False, it will try to re-download / re-chunk the file.",
    )

    class Config:
        use_enum_values = True


# The columns or labels to clear
# NOTE -> all the keys in columns_to_explode **must** have an entry here
columns_common_prefixes = {
    "content_type": "CONTENT_TYPE_",
    "decision_visibility": "DECISION_VISIBILITY_",
    "category_addition": "STATEMENT_CATEGORY_",
    "category_specification": "KEYWORD_",
    "category": "STATEMENT_CATEGORY_",
}
"""The common prefixes to remove from the columns having multiple values."""


class TDB_columnsFull(StrEnum):
    """Enum of all columns in the DSA Transparency Database and full daily dumps."""

    uuid = "uuid"
    decision_visibility = "decision_visibility"
    decision_visibility_other = "decision_visibility_other"
    end_date_visibility_restriction = "end_date_visibility_restriction"
    decision_monetary = "decision_monetary"
    decision_monetary_other = "decision_monetary_other"
    end_date_monetary_restriction = "end_date_monetary_restriction"
    decision_provision = "decision_provision"
    end_date_service_restriction = "end_date_service_restriction"
    decision_account = "decision_account"
    end_date_account_restriction = "end_date_account_restriction"
    account_type = "account_type"
    decision_ground = "decision_ground"
    decision_ground_reference_url = "decision_ground_reference_url"
    illegal_content_legal_ground = "illegal_content_legal_ground"
    illegal_content_explanation = "illegal_content_explanation"
    incompatible_content_ground = "incompatible_content_ground"
    incompatible_content_explanation = "incompatible_content_explanation"
    incompatible_content_illegal = "incompatible_content_illegal"
    category = "category"
    category_addition = "category_addition"
    category_specification = "category_specification"
    category_specification_other = "category_specification_other"
    content_type = "content_type"
    content_type_other = "content_type_other"
    content_language = "content_language"
    content_date = "content_date"
    territorial_scope = "territorial_scope"
    application_date = "application_date"
    decision_facts = "decision_facts"
    source_type = "source_type"
    source_identity = "source_identity"
    automated_detection = "automated_detection"
    automated_decision = "automated_decision"
    platform_name = "platform_name"
    platform_uid = "platform_uid"
    created_at = "created_at"


class TDB_columnsLight(StrEnum):
    """Enum of all columns in the light daily dump files."""

    uuid = "uuid"
    decision_visibility = "decision_visibility"
    decision_visibility_other = "decision_visibility_other"
    end_date_visibility_restriction = "end_date_visibility_restriction"
    decision_monetary = "decision_monetary"
    decision_monetary_other = "decision_monetary_other"
    end_date_monetary_restriction = "end_date_monetary_restriction"
    decision_provision = "decision_provision"
    end_date_service_restriction = "end_date_service_restriction"
    decision_account = "decision_account"
    end_date_account_restriction = "end_date_account_restriction"
    account_type = "account_type"
    decision_ground = "decision_ground"
    decision_ground_reference_url = "decision_ground_reference_url"
    illegal_content_legal_ground = "illegal_content_legal_ground"
    incompatible_content_ground = "incompatible_content_ground"
    incompatible_content_illegal = "incompatible_content_illegal"
    category = "category"
    category_addition = "category_addition"
    category_specification = "category_specification"
    category_specification_other = "category_specification_other"
    content_type = "content_type"
    content_type_other = "content_type_other"
    content_language = "content_language"
    content_date = "content_date"
    application_date = "application_date"
    source_type = "source_type"
    source_identity = "source_identity"
    automated_detection = "automated_detection"
    automated_decision = "automated_decision"
    platform_name = "platform_name"
    platform_uid = "platform_uid"
    created_at = "created_at"


class TDB_datetimeColumns(StrEnum):
    """Enum of all datetime columns in the DSA Transparency Database and full daily dumps."""

    content_date = "content_date"
    application_date = "application_date"
    created_at = "created_at"
    end_date_account_restriction = "end_date_account_restriction"
    end_date_monetary_restriction = "end_date_monetary_restriction"
    end_date_service_restriction = "end_date_service_restriction"
    end_date_visibility_restriction = "end_date_visibility_restriction"


class TDB_freetextColumns(StrEnum):
    decision_facts = "decision_facts"
    illegal_content_explanation = "illegal_content_explanation"
    incompatible_content_explanation = "incompatible_content_explanation"
    decision_visibility_other = "decision_visibility_other"
    decision_monetary_other = "decision_monetary_other"
    category_specification_other = "category_specification_other"
    content_type_other = "content_type_other"


# TODO: check with tests that TDB_columnsLight and TDB_datetimeColumns
# are subsets of TDB_columnsFull


class UseColumns(BaseModel):
    """The base models to validate columns to use."""

    columns: List[TDB_columnsFull] = Field(..., description="The list of columns to use.")

    class Config:
        use_enum_values = True


class DatetimeColumns(BaseModel):
    """The base models to validate the datetime columns to use."""

    columns: List[TDB_datetimeColumns] = Field(..., description="The list of datetime columns to use.")

    class Config:
        use_enum_values = True


all_columns = list(TDB_columnsFull.__members__)
"""The list of all the columns in the DSA Transparency Database and full daily dumps."""

all_columns_light = list(TDB_columnsLight.__members__)
"""The list of all the columns in the daily light dump files.
These are the same columns as the full file but the
`{'decision_facts', 'illegal_content_explanation',
'incompatible_content_explanation', 'territorial_scope'}` columns."""


class ContentType(StrEnum):
    """Enum of the content types."""

    CONTENT_TYPE_APP = "CONTENT_TYPE_APP"
    CONTENT_TYPE_AUDIO = "CONTENT_TYPE_AUDIO"
    CONTENT_TYPE_IMAGE = "CONTENT_TYPE_IMAGE"
    CONTENT_TYPE_PRODUCT = "CONTENT_TYPE_PRODUCT"
    CONTENT_TYPE_SYNTHETIC_MEDIA = "CONTENT_TYPE_SYNTHETIC_MEDIA"
    CONTENT_TYPE_TEXT = "CONTENT_TYPE_TEXT"
    CONTENT_TYPE_VIDEO = "CONTENT_TYPE_VIDEO"
    CONTENT_TYPE_OTHER = "CONTENT_TYPE_OTHER"

    # Extra content type, normalized from most common values in CONTENT_TYPE_OTHER
    CONTENT_TYPE_LINK = "CONTENT_TYPE_LINK"
    CONTENT_TYPE_ACCOUNT = "CONTENT_TYPE_ACCOUNT"
    CONTENT_TYPE_AD = "CONTENT_TYPE_AD"
    CONTENT_TYPE_STICKER = "CONTENT_TYPE_STICKER"
    CONTENT_TYPE_HASHTAG = "CONTENT_TYPE_HASHTAG"


CONTENT_TYPE_OTHER_NORMALIZATION = {
    ContentType.CONTENT_TYPE_LINK.value: ["Pin", "Link"],
    ContentType.CONTENT_TYPE_ACCOUNT.value: ["Account", "Profile"],
    ContentType.CONTENT_TYPE_AD.value: ["Advertisement", "Ad"],
    ContentType.CONTENT_TYPE_STICKER.value: ["Sticker"],
    ContentType.CONTENT_TYPE_HASHTAG.value: ["Hashtag"],
}


class DecisionVisibility(StrEnum):
    """Enum of the decision visibility."""

    DECISION_VISIBILITY_CONTENT_REMOVED = "DECISION_VISIBILITY_CONTENT_REMOVED"
    DECISION_VISIBILITY_CONTENT_DISABLED = "DECISION_VISIBILITY_CONTENT_DISABLED"
    DECISION_VISIBILITY_CONTENT_DEMOTED = "DECISION_VISIBILITY_CONTENT_DEMOTED"
    DECISION_VISIBILITY_CONTENT_AGE_RESTRICTED = "DECISION_VISIBILITY_CONTENT_AGE_RESTRICTED"
    DECISION_VISIBILITY_CONTENT_INTERACTION_RESTRICTED = "DECISION_VISIBILITY_CONTENT_INTERACTION_RESTRICTED"
    DECISION_VISIBILITY_CONTENT_LABELLED = "DECISION_VISIBILITY_CONTENT_LABELLED"
    DECISION_VISIBILITY_OTHER = "DECISION_VISIBILITY_OTHER"


DecisionVisibilityDict = {i.name: n for n, i in enumerate(DecisionVisibility)}


class DecisionMonetary(StrEnum):
    """Enum of the decision monetary."""

    DECISION_MONETARY_SUSPENSION = "DECISION_MONETARY_SUSPENSION"
    DECISION_MONETARY_TERMINATION = "DECISION_MONETARY_TERMINATION"
    DECISION_MONETARY_OTHER = "DECISION_MONETARY_OTHER"


class DecisionProvision(StrEnum):
    """Enum of the decision provision."""

    DECISION_PROVISION_PARTIAL_SUSPENSION = "DECISION_PROVISION_PARTIAL_SUSPENSION"
    DECISION_PROVISION_TOTAL_SUSPENSION = "DECISION_PROVISION_TOTAL_SUSPENSION"
    DECISION_PROVISION_PARTIAL_TERMINATION = "DECISION_PROVISION_PARTIAL_TERMINATION"
    DECISION_PROVISION_TOTAL_TERMINATION = "DECISION_PROVISION_TOTAL_TERMINATION"


class DecisionAccount(StrEnum):
    """Enum of the decision account."""

    DECISION_ACCOUNT_SUSPENDED = "DECISION_ACCOUNT_SUSPENDED"
    DECISION_ACCOUNT_TERMINATED = "DECISION_ACCOUNT_TERMINATED"


class DecisionGround(StrEnum):
    """Enum of the decision ground."""

    DECISION_GROUND_ILLEGAL_CONTENT = "DECISION_GROUND_ILLEGAL_CONTENT"
    DECISION_GROUND_INCOMPATIBLE_CONTENT = "DECISION_GROUND_INCOMPATIBLE_CONTENT"


class AutomatedDetection(StrEnum):
    """Enum of the automated detection."""

    Yes = "Yes"
    No = "No"


class AutomatedDecision(StrEnum):
    """Enum of the automated decision."""

    AUTOMATED_DECISION_FULLY = "AUTOMATED_DECISION_FULLY"
    AUTOMATED_DECISION_PARTIALLY = "AUTOMATED_DECISION_PARTIALLY"
    AUTOMATED_DECISION_NOT_AUTOMATED = "AUTOMATED_DECISION_NOT_AUTOMATED"


class SourceType(StrEnum):
    """Enum of the source type."""

    SOURCE_ARTICLE_16 = "SOURCE_ARTICLE_16"
    SOURCE_TRUSTED_FLAGGER = "SOURCE_TRUSTED_FLAGGER"
    SOURCE_TYPE_OTHER_NOTIFICATION = "SOURCE_TYPE_OTHER_NOTIFICATION"
    SOURCE_VOLUNTARY = "SOURCE_VOLUNTARY"


class AccountType(StrEnum):
    """Enum of the account type."""

    ACCOUNT_TYPE_BUSINESS = "ACCOUNT_TYPE_BUSINESS"
    ACCOUNT_TYPE_PRIVATE = "ACCOUNT_TYPE_PRIVATE"


class IncompatibleContentIllegal(StrEnum):
    """Enum of the incompatible content illegal."""

    Yes = "Yes"
    No = "No"


class TerritorialScope(StrEnum):
    """Enum of the territorial scope."""

    EU = "EU"
    EEA = "EEA"
    EEA_no_IS = "EEA_no_IS"
    AT = "AT"
    BE = "BE"
    BG = "BG"
    CY = "CY"
    CZ = "CZ"
    DE = "DE"
    DK = "DK"
    EE = "EE"
    ES = "ES"
    FI = "FI"
    FR = "FR"
    GR = "GR"
    HR = "HR"
    HU = "HU"
    IE = "IE"
    IS = "IS"
    IT = "IT"
    LI = "LI"
    LT = "LT"
    LU = "LU"
    LV = "LV"
    MT = "MT"
    NL = "NL"
    NO = "NO"
    PL = "PL"
    PT = "PT"
    RO = "RO"
    SE = "SE"
    SI = "SI"
    SK = "SK"


class ContentLanguage(StrEnum):
    """Enum of the content language."""

    EN = "EN"
    BG = "BG"
    HR = "HR"
    CS = "CS"
    DA = "DA"
    NL = "NL"
    ET = "ET"
    FI = "FI"
    FR = "FR"
    DE = "DE"
    EL = "EL"
    HU = "HU"
    GA = "GA"
    IT = "IT"
    LV = "LV"
    LT = "LT"
    MT = "MT"
    PL = "PL"
    PT = "PT"
    RO = "RO"
    SK = "SK"
    SL = "SL"
    ES = "ES"
    SV = "SV"


class Category(StrEnum):
    STATEMENT_CATEGORY_ANIMAL_WELFARE = "STATEMENT_CATEGORY_ANIMAL_WELFARE"
    STATEMENT_CATEGORY_DATA_PROTECTION_AND_PRIVACY_VIOLATIONS = (
        "STATEMENT_CATEGORY_DATA_PROTECTION_AND_PRIVACY_VIOLATIONS"
    )
    STATEMENT_CATEGORY_ILLEGAL_OR_HARMFUL_SPEECH = "STATEMENT_CATEGORY_ILLEGAL_OR_HARMFUL_SPEECH"
    STATEMENT_CATEGORY_INTELLECTUAL_PROPERTY_INFRINGEMENTS = "STATEMENT_CATEGORY_INTELLECTUAL_PROPERTY_INFRINGEMENTS"
    STATEMENT_CATEGORY_NEGATIVE_EFFECTS_ON_CIVIC_DISCOURSE_OR_ELECTIONS = (
        "STATEMENT_CATEGORY_NEGATIVE_EFFECTS_ON_CIVIC_DISCOURSE_OR_ELECTIONS"
    )
    STATEMENT_CATEGORY_NON_CONSENSUAL_BEHAVIOUR = "STATEMENT_CATEGORY_NON_CONSENSUAL_BEHAVIOUR"
    STATEMENT_CATEGORY_PORNOGRAPHY_OR_SEXUALIZED_CONTENT = "STATEMENT_CATEGORY_PORTNOGRAPHY_OR_SEXUALIZED_CONTENT"
    STATEMENT_CATEGORY_PROTECTION_OF_MINORS = "STATEMENT_CATEGORY_PROTECTION_OF_MINORS"
    STATEMENT_CATEGORY_RISK_FOR_PUBLIC_SECURITY = "STATEMENT_CATEGORY_RISK_FOR_PUBLIC_SECURITY"
    STATEMENT_CATEGORY_SCAMS_AND_FRAUD = "STATEMENT_CATEGORY_SCAMS_AND_FRAUD"
    STATEMENT_CATEGORY_SELF_HARM = "STATEMENT_CATEGORY_SELF_HARM"
    STATEMENT_CATEGORY_SCOPE_OF_PLATFORM_SERVICE = "STATEMENT_CATEGORY_SCOPE_OF_PLATFORM_SERVICE"
    STATEMENT_CATEGORY_UNSAFE_AND_ILLEGAL_PRODUCTS = "STATEMENT_CATEGORY_UNSAFE_AND_ILLEGAL_PRODUCTS"
    STATEMENT_CATEGORY_VIOLENCE = "STATEMENT_CATEGORY_VIOLENCE"


CategoryDict = {i.name: n for n, i in enumerate(Category)}


class CategoryAddition(BaseModel):
    """The base models to validate the category addition."""

    category_addition: List[Category] = Field(..., description="The list of category addition.")

    class Config:
        use_enum_values = True


class Keyword(StrEnum):
    KEYWORD_ANIMAL_HARM = "KEYWORD_ANIMAL_HARM"
    KEYWORD_ADULT_SEXUAL_MATERIAL = "KEYWORD_ADULT_SEXUAL_MATERIAL"
    KEYWORD_AGE_SPECIFIC_RESTRICTIONS_MINORS = "KEYWORD_AGE_SPECIFIC_RESTRICTIONS_MINORS"
    KEYWORD_AGE_SPECIFIC_RESTRICTIONS = "KEYWORD_AGE_SPECIFIC_RESTRICTIONS"
    KEYWORD_BIOMETRIC_DATA_BREACH = "KEYWORD_BIOMETRIC_DATA_BREACH"
    KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL = "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL"
    KEYWORD_CONTENT_PROMOTING_EATING_DISORDERS = "KEYWORD_CONTENT_PROMOTING_EATING_DISORDERS"
    KEYWORD_COORDINATED_HARM = "KEYWORD_COORDINATED_HARM"
    KEYWORD_COPYRIGHT_INFRINGEMENT = "KEYWORD_COPYRIGHT_INFRINGEMENT"
    KEYWORD_DANGEROUS_TOYS = "KEYWORD_DANGEROUS_TOYS"
    KEYWORD_DATA_FALSIFICATION = "KEYWORD_DATA_FALSIFICATION"
    KEYWORD_DEFAMATION = "KEYWORD_DEFAMATION"
    KEYWORD_DESIGN_INFRINGEMENT = "KEYWORD_DESIGN_INFRINGEMENT"
    KEYWORD_DISCRIMINATION = "KEYWORD_DISCRIMINATION"
    KEYWORD_DISINFORMATION = "KEYWORD_DISINFORMATION"
    KEYWORD_FOREIGN_INFORMATION_MANIPULATION = "KEYWORD_FOREIGN_INFORMATION_MANIPULATION"
    KEYWORD_GENDER_BASED_VIOLENCE = "KEYWORD_GENDER_BASED_VIOLENCE"
    KEYWORD_GEOGRAPHIC_INDICATIONS_INFRINGEMENT = "KEYWORD_GEOGRAPHIC_INDICATIONS_INFRINGEMENT"
    KEYWORD_GEOGRAPHICAL_REQUIREMENTS = "KEYWORD_GEOGRAPHICAL_REQUIREMENTS"
    KEYWORD_GOODS_SERVICES_NOT_PERMITTED = "KEYWORD_GOODS_SERVICES_NOT_PERMITTED"
    KEYWORD_GROOMING_SEXUAL_ENTICEMENT_MINORS = "KEYWORD_GROOMING_SEXUAL_ENTICEMENT_MINORS"
    KEYWORD_HATE_SPEECH = "KEYWORD_HATE_SPEECH"
    KEYWORD_HUMAN_EXPLOITATION = "KEYWORD_HUMAN_EXPLOITATION"
    KEYWORD_HUMAN_TRAFFICKING = "KEYWORD_HUMAN_TRAFFICKING"
    KEYWORD_ILLEGAL_ORGANIZATIONS = "KEYWORD_ILLEGAL_ORGANIZATIONS"
    KEYWORD_IMAGE_BASED_SEXUAL_ABUSE = "KEYWORD_IMAGE_BASED_SEXUAL_ABUSE"
    KEYWORD_IMPERSONATION_ACCOUNT_HIJACKING = "KEYWORD_IMPERSONATION_ACCOUNT_HIJACKING"
    KEYWORD_INAUTHENTIC_ACCOUNTS = "KEYWORD_INAUTHENTIC_ACCOUNTS"
    KEYWORD_INAUTHENTIC_LISTINGS = "KEYWORD_INAUTHENTIC_LISTINGS"
    KEYWORD_INAUTHENTIC_USER_REVIEWS = "KEYWORD_INAUTHENTIC_USER_REVIEWS"
    KEYWORD_INCITEMENT_VIOLENCE_HATRED = "KEYWORD_INCITEMENT_VIOLENCE_HATRED"
    KEYWORD_INSUFFICIENT_INFORMATION_TRADERS = "KEYWORD_INSUFFICIENT_INFORMATION_TRADERS"
    KEYWORD_LANGUAGE_REQUIREMENTS = "KEYWORD_LANGUAGE_REQUIREMENTS"
    KEYWORD_MISINFORMATION = "KEYWORD_MISINFORMATION"
    KEYWORD_MISSING_PROCESSING_GROUND = "KEYWORD_MISSING_PROCESSING_GROUND"
    KEYWORD_NON_CONSENSUAL_IMAGE_SHARING = "KEYWORD_NON_CONSENSUAL_IMAGE_SHARING"
    KEYWORD_NON_CONSENSUAL_ITEMS_DEEPFAKE = "KEYWORD_NON_CONSENSUAL_ITEMS_DEEPFAKE"
    KEYWORD_NUDITY = "KEYWORD_NUDITY"
    KEYWORD_ONLINE_BULLYING_INTIMIDATION = "KEYWORD_ONLINE_BULLYING_INTIMIDATION"
    KEYWORD_PATENT_INFRINGEMENT = "KEYWORD_PATENT_INFRINGEMENT"
    KEYWORD_PHISHING = "KEYWORD_PHISHING"
    KEYWORD_PYRAMID_SCHEMES = "KEYWORD_PYRAMID_SCHEMES"
    KEYWORD_REGULATED_GOODS_SERVICES = "KEYWORD_REGULATED_GOODS_SERVICES"
    KEYWORD_RIGHT_TO_BE_FORGOTTEN = "KEYWORD_RIGHT_TO_BE_FORGOTTEN"
    KEYWORD_RISK_ENVIRONMENTAL_DAMAGE = "KEYWORD_RISK_ENVIRONMENTAL_DAMAGE"
    KEYWORD_RISK_PUBLIC_HEALTH = "KEYWORD_RISK_PUBLIC_HEALTH"
    KEYWORD_SELF_MUTILATION = "KEYWORD_SELF_MUTILATION"
    KEYWORD_STALKING = "KEYWORD_STALKING"
    KEYWORD_SUICIDE = "KEYWORD_SUICIDE"
    KEYWORD_TERRORIST_CONTENT = "KEYWORD_TERRORIST_CONTENT"
    KEYWORD_TRADE_SECRET_INFRINGEMENT = "KEYWORD_TRADE_SECRET_INFRINGEMENT"  # nosec B105
    KEYWORD_TRADEMARK_INFRINGEMENT = "KEYWORD_TRADEMARK_INFRINGEMENT"
    KEYWORD_UNLAWFUL_SALE_ANIMALS = "KEYWORD_UNLAWFUL_SALE_ANIMALS"
    KEYWORD_UNSAFE_CHALLENGES = "KEYWORD_UNSAFE_CHALLENGES"
    KEYWORD_OTHER = "KEYWORD_OTHER"


KeywordDict = {i.name: n for (n, i) in enumerate(Keyword)}


class CategorySpecification(BaseModel):
    """The base models to validate the category specification."""

    category_specification: List[Keyword] = Field(..., description="The list of category specification.")

    class Config:
        use_enum_values = True


# These are the array columns to possible values mappings
# If horizontally_explode_columns=False, then these coluns will just be sorted.
columns_to_explode = {
    "content_type": list(ContentType.__members__),
    "decision_visibility": list(DecisionVisibility.__members__),
    "category_addition": list(Category.__members__),
    "category_specification": list(Keyword.__members__),
}
"""The dictionary reporting the name of the columns and the possible values it can feature."""

EXPLODED_COLUMNS = unique(
    Enum("EXPLODED_COLUMNS", OrderedDict([(c, c) for k, v in columns_to_explode.items() for c in v]))
)
"""The set of columns that we can obtain when exploding the columns."""

# The territorial scopes, I added EEA_no_IS (which is EEA without Iceland) as it is apparently used by everyone.
# These labels will replace the set of countries they match with
territorial_scopes = {
    "EU": set([
        "AT",
        "BE",
        "BG",
        "CY",
        "CZ",
        "DE",
        "DK",
        "EE",
        "ES",
        "FI",
        "FR",
        "GR",
        "HR",
        "HU",
        "IE",
        "IT",
        "LT",
        "LU",
        "LV",
        "MT",
        "NL",
        "PL",
        "PT",
        "RO",
        "SE",
        "SI",
        "SK",
    ]),
    "EEA": set([
        "AT",
        "BE",
        "BG",
        "CY",
        "CZ",
        "DE",
        "DK",
        "EE",
        "ES",
        "FI",
        "FR",
        "GR",
        "HR",
        "HU",
        "IE",
        "IS",
        "IT",
        "LI",
        "LT",
        "LU",
        "LV",
        "MT",
        "NL",
        "NO",
        "PL",
        "PT",
        "RO",
        "SE",
        "SI",
        "SK",
    ]),
    "EEA_no_IS": set([
        "AT",
        "BE",
        "BG",
        "CY",
        "CZ",
        "DE",
        "DK",
        "EE",
        "ES",
        "FI",
        "FR",
        "GR",
        "HR",
        "HU",
        "IE",
        "IT",
        "LI",
        "LT",
        "LU",
        "LV",
        "MT",
        "NL",
        "NO",
        "PL",
        "PT",
        "RO",
        "SE",
        "SI",
        "SK",
    ]),
}
"""The dictionary reporting the name of the territorial scope and the set of countries it matches.
**TODO** Must be ported to a pydantic type."""

# Workaround to combine the enums
# https://stackoverflow.com/questions/46073413/python-enum-combination
RawAndExplodedColumn = unique(
    Enum(
        "RawAndExplodedColumn",
        OrderedDict([
            (i.name, i.name)
            for c, i in enumerate(
                chain(
                    TDB_columnsFull,
                    ContentType,
                    DecisionAccount,
                    DecisionMonetary,
                    DecisionProvision,
                    DecisionVisibility,
                    Category,
                    Keyword,
                    EXPLODED_COLUMNS,
                )
            )
        ]),
    )
)


class RawAndExplodedColumns(BaseModel):
    """The base models to validate the columns found when importing and/or exploding."""

    columns: List[RawAndExplodedColumn] = Field(
        ..., description="The list of columns potentially available as grouping keys."
    )

    class Config:
        use_enum_values = True


class InputFileFormat(StrEnum):
    """Enum of the input file formats."""

    csv = "csv"
    parquet = "parquet"


class AggregateFileFormat(StrEnum):
    """Enum of the aggregate file formats."""

    pickle = "pickle"
    parquet = "parquet"
    csv = "csv"


class AggregateWriteMode(StrEnum):
    """Enum of the aggregate modes for the output file."""

    append = "append"
    overwrite = "overwrite"
    error = "error"


class BooleanOperator(StrEnum):
    """Enum of the boolean operators."""

    AND = "AND"
    OR = "OR"


class FilteringConfig(BaseModel):
    """Configuration for the aggregation of data."""

    input_format: InputFileFormat = Field(InputFileFormat.parquet.name, description="Input file format.")
    output_format: AggregateFileFormat = Field(AggregateFileFormat.parquet.name, description="Output file format.")
    write_mode: AggregateWriteMode = Field(
        AggregateWriteMode.overwrite.name, description="Write mode for the output file."
    )
    delete_original_columns: bool = Field(False, description="Delete the original columns when horizontally exploding.")
    horizontally_explode_columns: bool = Field(False, description="Horizontally explode the columns.")
    normalize_platform_name: bool = Field(
        False, description="Whether to coalesce platform names when it changed over time."
    )
    normalize_content_type_other: bool = Field(False, description="Whether to normalize the content type other.")
    fillna_str_value: Optional[Union[str, None]] = Field(None, description="Value to use for filling NA values.")
    fillna_bool_value: Optional[Union[bool, None]] = Field(False, description="Value to use for filling NA values.")
    content_date_range: Optional[List[datetime]] = Field(
        None, description="Content date range to use for filtering the input files."
    )
    decision_date_range: Optional[List[datetime]] = Field(
        None, description="Decision date range to use for filtering the input files."
    )
    created_at_date_range: Optional[List[datetime]] = Field(
        None, description="Created at date range to use for filtering the input files."
    )
    platforms_to_exclude: Optional[List[str]] = Field(None, description="Platforms to exclude.")
    platforms_to_include: Optional[List[str]] = Field(None, description="Platforms to exclude.")
    created_at_dt_floor: Optional[Union[str, None]] = Field(
        "day",
        description="The argument to pass to pyspark sql date_trunc when flooring the creation date. By default `day`",
    )
    columns_to_import: List[TDB_columnsFull] = Field(
        [c.name for c in TDB_columnsFull if not getattr(TDB_datetimeColumns, c, None)],
        description="Columns to import from the input files or df **before** exploding.",
    )
    columns_datetime: List[TDB_datetimeColumns] = Field(
        [c.name for c in TDB_datetimeColumns], description="Columns to convert to datetime."
    )
    upstream_sampling: Optional[float] = Field(
        None,
        description="The fraction (if 0 < x < 1) or the number of rows (if x >= 1, will be rounded) to sample from the input files **BEFORE** filtering. Leave to null or <=0 to keep all the rows.",
    )
    downstream_sampling: Optional[float] = Field(
        None,
        description="The fraction (if 0 < x < 1) or the number of rows (if x >= 1, will be rounded) to sample from the input files **AFTER** filtering. Leave to null or <=0 to keep all the rows.",
    )

    bool_columns_to_check: Optional[List[RawAndExplodedColumn]] = Field(
        None, description="Columns to check for boolean values."
    )
    bool_columns_to_check_operator: BooleanOperator = Field(
        BooleanOperator.OR.name, description="The operator to use when checking the boolean columns."
    )
    decision_monetary: Optional[List[DecisionMonetary]] = Field(None, description="The decision monetary to filter.")
    decision_provision: Optional[List[DecisionProvision]] = Field(None, description="The decision provision to filter.")
    decision_account: Optional[List[DecisionAccount]] = Field(None, description="The decision account to filter.")
    decision_visibility: Optional[List[DecisionVisibility]] = Field(
        None, description="The decision visibility to filter."
    )
    category: Optional[List[Category]] = Field(None, description="The list of category(ies) to filter.")
    decision_ground: Optional[List[DecisionGround]] = Field(None, description="The decision ground to filter.")
    automated_detection: Optional[List[AutomatedDetection]] = Field(
        None, description="The automated detection to filter."
    )
    automated_decision: Optional[List[AutomatedDecision]] = Field(None, description="The automated decision to filter.")
    source_type: Optional[List[SourceType]] = Field(None, description="The source type to filter.")
    account_type: Optional[List[AccountType]] = Field(None, description="The account type to filter.")
    incompatible_content_illegal: Optional[List[IncompatibleContentIllegal]] = Field(
        None, description="The incompatible content illegal to filter."
    )
    content_language: Optional[List[ContentLanguage]] = Field(None, description="The content language to filter.")
    content_type: Optional[List[ContentType]] = Field(None, description="The content type to filter.")
    category_addition: Optional[List[Category]] = Field(None, description="The category addition to filter.")
    category_specification: Optional[List[Keyword]] = Field(None, description="The category specification to filter.")
    territorial_scope: Optional[List[TerritorialScope]] = Field(None, description="The territorial scope to filter.")

    decision_visibility_other: Optional[str] = Field(None, description="The decision visibility other to filter.")
    decision_visibility_other_to_lower: bool = Field(
        False, description="Whether the decision visibility other shall be cast to lower text before being analyzed.."
    )
    decision_monetary_other: Optional[str] = Field(None, description="The decision monetary other to filter.")
    decision_monetary_other_to_lower: bool = Field(
        False, description="Whether the decision monetary other shall be cast to lower text before being analyzed.."
    )
    decision_facts: Optional[str] = Field(None, description="The decision facts to filter.")
    decision_facts_to_lower: bool = Field(
        False, description="Whether the decision facts shall be cast to lower text before being analyzed.."
    )
    decision_ground_reference_url: Optional[str] = Field(
        None, description="The decision ground reference url to filter."
    )
    decision_ground_reference_url_to_lower: bool = Field(
        False,
        description="Whether the decision ground reference url shall be cast to lower text before being analyzed..",
    )
    illegal_content_legal_ground: Optional[str] = Field(None, description="The illegal content legal ground to filter.")
    illegal_content_legal_ground_to_lower: bool = Field(
        False,
        description="Whether the illegal content legal ground shall be cast to lower text before being analyzed..",
    )
    illegal_content_explanation: Optional[str] = Field(None, description="The illegal content explanation to filter.")
    illegal_content_explanation_to_lower: bool = Field(
        False, description="Whether the illegal content explanation shall be cast to lower text before being analyzed.."
    )
    incompatible_content_ground: Optional[str] = Field(None, description="The incompatible content ground to filter.")
    incompatible_content_ground_to_lower: bool = Field(
        False, description="Whether the incompatible content ground shall be cast to lower text before being analyzed.."
    )
    incompatible_content_explanation: Optional[str] = Field(
        None, description="The incompatible content explanation to filter."
    )
    incompatible_content_explanation_to_lower: bool = Field(
        False,
        description="Whether the incompatible content explanation shall be cast to lower text before being analyzed..",
    )
    content_type_other: Optional[str] = Field(None, description="The content type other to filter.")
    content_type_other_to_lower: bool = Field(
        False, description="Whether the content type other shall be cast to lower text before being analyzed.."
    )
    category_specification_other: Optional[str] = Field(None, description="The category specification other to filter.")
    category_specification_other_to_lower: bool = Field(
        False,
        description="Whether the category specification other shall be cast to lower text before being analyzed..",
    )
    source_identity: Optional[str] = Field(None, description="The source identity to filter.")
    source_identity_to_lower: bool = Field(
        False, description="Whether the source identity shall be cast to lower text before being analyzed.."
    )

    end_date_account_restriction: Optional[datetime] = Field(
        None, description="The end date account restriction to filter."
    )
    end_date_monetary_restriction: Optional[datetime] = Field(
        None, description="The end date monetary restriction to filter."
    )
    end_date_service_restriction: Optional[datetime] = Field(
        None, description="The end date service restriction to filter."
    )
    end_date_visibility_restriction: Optional[datetime] = Field(
        None, description="The end date visibility restriction to filter."
    )

    columns_to_fill_str: List[RawAndExplodedColumn] = Field([], description="Columns to fillna with a string.")
    columns_to_fill_bool: List[RawAndExplodedColumn] = Field(
        [c for k, v in columns_to_explode.items() for c in v], description="Columns to fillna with a bool."
    )

    class Config:
        use_enum_values = True


class AggregationConfig(BaseModel):
    """Configuration for the aggregation of data."""

    input_format: InputFileFormat = Field(InputFileFormat.parquet.name, description="Input file format.")
    delete_original_columns: bool = Field(False, description="Delete the original columns when horizontally exploding.")
    horizontally_explode_columns: bool = Field(False, description="Horizontally explode the columns.")
    normalize_platform_name: bool = Field(
        False, description="Whether to coalesce platform names when it changed over time."
    )
    normalize_content_type_other: bool = Field(False, description="Whether to normalize the content type other.")
    output_format: AggregateFileFormat = Field(AggregateFileFormat.parquet.name, description="Output file format.")
    fillna_str_value: Union[str, None] = Field(None, description="Value to use for filling NA values.")
    fillna_bool_value: Union[bool, None] = Field(False, description="Value to use for filling NA values.")
    content_date_range: Optional[List[datetime]] = Field(
        None, description="Content date range to use for filtering the input files."
    )
    decision_date_range: Optional[List[datetime]] = Field(
        None, description="Decision date range to use for filtering the input files."
    )
    created_at_date_range: Optional[List[datetime]] = Field(
        None, description="Created at date range to use for filtering the input files."
    )
    platforms_to_exclude: Optional[List[str]] = Field(None, description="Platforms to exclude.")
    columns_to_import: List[TDB_columnsFull] = Field(
        [c.name for c in TDB_columnsFull if c not in TDB_datetimeColumns],
        description="Columns to import from the input files.",
    )
    columns_datetime: List[TDB_datetimeColumns] = Field(
        [c.name for c in TDB_datetimeColumns], description="Columns to convert to datetime."
    )
    columns_to_group: Optional[List[RawAndExplodedColumn]] = Field(None, description="Columns to groupby.")
    columns_to_fill_str: List[RawAndExplodedColumn] = Field([], description="Columns to fillna with a string.")
    columns_to_fill_bool: List[RawAndExplodedColumn] = Field(
        [c for k, v in columns_to_explode.items() for c in v], description="Columns to fillna with a bool."
    )
    compute_time_to_action: bool = Field(False, description="Compute the average time to action and report.")
    compute_restriction_duration: bool = Field(
        False, description="Compute the restriction duration time when available."
    )
    write_mode: AggregateWriteMode = Field(
        AggregateWriteMode.overwrite.name, description="Write mode for the output file."
    )
    created_at_dt_floor: Union[str, None] = Field(
        "day",
        description="The argument to pass to pyspark sql date_trunc when flooring the creation date. By default `day`",
    )

    class Config:
        use_enum_values = True


class LoadFileArguments(BaseModel):
    """The base models to validate the load file arguments."""

    dump_files_pattern: Union[str, List] = Field(..., description="The pattern of the daily dumps to load.")
    columns_to_import: Optional[List[TDB_columnsFull]] = Field(
        [c.name for c in TDB_columnsFull if c not in TDB_datetimeColumns],
        description="The columns to import from the daily dumps.",
    )
    columns_datetime: Optional[List[TDB_datetimeColumns]] = Field(
        [c.name for c in TDB_datetimeColumns], description="The columns to convert to datetime."
    )
    content_date_range: Optional[List[datetime]] = Field(
        None, description="Content date range to use for filtering the input files."
    )
    decision_date_range: Optional[List[datetime]] = Field(
        None, description="Decision date range to use for filtering the input files."
    )
    created_at_date_range: Optional[List[datetime]] = Field(
        None, description="Created at date range to use for filtering the input files."
    )
    input_format: TDB_chunkFormat = Field(TDB_chunkFormat.csv, description="The format of the input files.")
    del_original: bool = Field(True, description="Whether to delete the original files after loading.")
    explode_cols: bool = Field(True, description="Whether to horizontally explode the columns.")
    fillna_str: Optional[Union[str, None]] = Field(None, description="The value to use for filling NA values.")
    fillna_bool: Optional[Union[bool, None]] = Field(False, description="The value to use for filling NA values.")
    columns_to_fill_str: List[RawAndExplodedColumn] = Field([], description="Columns to fillna with a string.")
    columns_to_fill_bool: List[RawAndExplodedColumn] = Field(
        [c for k, v in columns_to_explode.items() for c in v], description="Columns to fillna with a bool."
    )
    compute_time_to_action: bool = Field(False, description="Compute the average time to action and report.")
    compute_restriction_duration: bool = Field(
        False, description="Compute the restriction duration time when available."
    )
    normalize_platform_name: bool = Field(
        False, description="Whether to coalesce platform names when it changed over time."
    )
    normalize_content_type_other: bool = Field(False, description="Whether to normalize the content type other values.")

    class Config:
        use_enum_values = True


# Schemas

TDB_AGGREGATED_RAW_SCHEMA = {
    "uuid": "string",
    "count": np.uint64,
    "__null_dask_index__": np.int64,
    "time_to_action": np.float64,
    "time_to_report": np.float64,
    "time_to_upload": np.float64,
    "restriction_duration": np.float64,
}
TDB_AGGREGATED_RAW_SCHEMA.update({c: "datetime64[ms]" for c in TDB_datetimeColumns._member_names_})
TDB_AGGREGATED_RAW_SCHEMA.update({c: bool for k, v in columns_to_explode.items() for c in v})
TDB_AGGREGATED_RAW_SCHEMA.update({
    c: "string" for c in TDB_columnsFull._member_names_ if c not in TDB_AGGREGATED_RAW_SCHEMA
})

TDB_PD_TO_PA_LOOKUP = {
    "datetime": pa.timestamp("ms"),
    "datetime64[ms]": pa.timestamp("ms"),
    np.uint64: pa.uint64(),
    np.int64: pa.int64(),
    np.float64: pa.float64(),
    "string": pa.string(),
    bool: pa.bool_(),
}

# Dates
# format is 'ISO8601'
datetime_columns = list(TDB_datetimeColumns.__members__)
"""The list of the columns containing datetime values."""

datetime_format = "ISO8601"
"""The format of the datetime values."""

datetime_format_strftime = "%Y-%m-%d %H:%M:%S"
"""The format of the datetime values for the strftime method."""


# Platforms names normalization
# some platforms changed names over time. This dict keeps track of the
# new name => old name mapping to coalesce all the SOR under a single
# (initial) platform name.
coalesce_platforms_names = {
    "Amazon Store": "Amazon",
}

# Default spark configuration to be used
defaultSparkConf = SparkConf()
defaultSparkConf.set("spark.sql.session.timeZone", "UTC")
defaultSparkConf.set("spark.driver.host", "127.0.0.1")
defaultSparkConf.set("spark.driver.maxResultSize", "10g")
defaultSparkConf.set("spark.driver.memory", "%dg" % int(0.8 * psutil.virtual_memory().total / (1024**3)))
defaultSparkConf.setAppName("sor-spark")
defaultSparkConf.set("spark.sql.caseSensitive", "true")

CELERY_TASK_QUEUE = "dsa_tdb_queue"
"""The name of the queue to use for the celery tasks."""
