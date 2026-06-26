
def parse_arguments():
    import argparse

    parser = argparse.ArgumentParser(description='Extract data from Analysis Services.')

    parser.add_argument(
        '--project-id',
        dest='project_id', 
        required=True, 
        help='GCP project ID.')

    parser.add_argument(
        '--destination-bucket',
        dest='destination_bucket', 
        required=True, 
        help='Bucket name to write data to.')

    parser.add_argument(
        '--server-name',
        dest='server_name', 
        required=True, 
        help='Analysis Services server name.')

    parser.add_argument(
        '--catalog', 
        dest='catalog', 
        required=True, 
        help='Database catalog.')

    parser.add_argument(
        '--user-id-secret',
        dest='user_id_secret', 
        required=True, 
        help='The name of the secret manager secret storing the Matrix username.'
    )

    parser.add_argument(
        '--password-secret',
        dest='password_secret', 
        required=True, 
        help='The name of the secret manager secret storing the Matrix password.')

    parser.add_argument(
        '--client-ids',
        dest='client_ids',
        required=True,
        help='Client name.'
    )
    parser.add_argument(
        '--client-name',
        dest='client_name',
        required=True,
        help='Client name.'
    )

    parser.add_argument(
        '--calendar-week',
        dest='calendar_week',
        required=False,
        help='Calendar week (for legacy compatibility)'
    )

    parser.add_argument(
        '--source-table',
        dest='source_table',
        required=True,
        help='The name of the source table to extract'
    )
    parser.add_argument(
        '--start-date',
        dest='start_date',
        required=False,
        help='Start date in YYYY-MM-DD format (used for data extract filters)'
    )

    parser.add_argument(
        '--end-date',
        dest='end_date',
        required=False,
        help='End date in YYYY-MM-DD format (used for data extract filters)'
    )
    
    parser.add_argument(
        '--output-path',
        dest='output_path',
        required=False,
        help='Custom GCS output path (for hive-partitioned structure)'
    )

    parser.add_argument(
        '--output-filename',
        dest='output_filename',
        required=False,
        default='booking_spot.csv',
        help='Output filename (default: booking_spot.csv)'
    )

    args = parser.parse_args()

    return args


query_lookup = [
    {
        'source_table_name': 'booking_spot_2',
        'destination_table_name': 'booking_spot_2', 
        'query': """
            EVALUATE
            SUMMARIZECOLUMNS(
                'Booking Spot'[SpotId],
                'Date'[DateId],
                'Date'[Month Of Year],
                'Date'[QuarterOfYear],
                'Date'[Calendar Quarter],
                'Date'[Day Of Week],
                'Date'[Day Of Year],
                'Date'[Day Of Month],
                'Date'[Commencing Week Year Start Date],
                'Date'[Commencing Week Year End Date],
                'Date'[Is Weekend],
                'Date'[Is Work Day],
                'Date'[Calendar Date],
                'Date'[Calendar Year],
                'Date'[Calendar Year Month],
                'Date'[Calendar Week],
                'Date'[Quarter Name],
                'Date'[Week],
                'Date'[Weekly Commencing Month],
                'Date'[YearQuarter],
                'Buying Brief'[Buying Brief Unique Key],
                'Buying Brief'[Buying Brief - Campaign Code],
                'Buying Brief'[Buying Brief],
                'Buying Brief'[Campaign Start Date],
                'Buying Brief'[Campaign End Date],
                'Actual Product Category'[Actual Product Category Code],
                'Actual Product Category'[Actual Product Category],
                'Actual Product Sub Category'[Actual Product Sub Category Code],
                'Actual Product Sub Category'[Actual Product Sub Category],
                'Ad Position Group'[Ad Position Group Code],
                'Ad Position Group'[Ad Position Group],
                'Agency'[AgencyId],
                'Agency'[Agency Code],
                'Agency'[Agency],
                'Agency'[Master Agency],
                'Agency'[Master Agency Code],
                'Booking Daypart'[Booking Daypart Code],
                'Booking Daypart'[Booking Daypart],
                'Booking Daypart'[Is Booking Prime Time],
                'Brand'[Brand Code],
                'Brand'[Brand],
                'Buy Type'[Buy Type],
                'Client'[ClientId],
                'Client'[Client Code],
                'Client'[Client],
                'Client'[Master Client],
                'Client'[Master Client Code],
                'Colour'[Colour Code],
                'Colour'[Colour],
                'Digital Creative Type'[Digital Creative Type Code],
                'Digital Creative Type'[Digital Creative Type],
                'Duration'[Duration Code],
                'Duration'[Duration],
                'Duration'[Is Expenditure],
                'Duration'[DurationLength],
                'Format Type'[Format Type Code],
                'Format Type'[Format Type],
                'Market'[Market Code],
                'Market'[Market],
                'Market'[Is Market Rating Only],
                'Market'[Country Code],
                'Market'[Country],
                'Market'[Market Level Name],
                'Market'[Default Panel Code],
                'Market'[City Province English Name],
                'Market'[City Province],
                'Media Vehicle'[Monitor Filter Pure Local TV],
                'Media Vehicle'[Monitor Filter Rating Mediums And Time Only],
                'Media Vehicle'[Media Vehicle Code],
                'Media Vehicle'[Media Vehicle],
                'Media Vehicle'[Media Subtype],
                'Media Vehicle'[Media Type],
                'Media Vehicle'[Is Media Vehicle Preferred Supplier],
                'Media Vehicle'[Media Frequency],
                'Media Vehicle'[Media Category],
                'Media Vehicle'[Language],
                'Media Vehicle'[Network Code],
                'Media Vehicle'[Media Spending Market Name],
                'Media Vehicle'[Master Network],
                'Media Vehicle'[Is Network Preferred Supplier],
                'Media Vehicle'[Is Master Network Preferred Supplier],
                'Media Vehicle'[Media Group],
                'Media Vehicle'[Network],
                'Media Vendor'[Media Vendor],
                'Media Vendor'[Master Media Vendor],
                'Media Vendor'[Is Vendor Preferred Supplier],
                'Media Vendor'[Is Master Vendor Preferred Supplier],
                'Package'[Package],
                'Panel'[Performance Panel],
                'Panel'[IsDefaultPanel],
                'Placement'[Placement],
                'Preferred Supplier'[Preferred Supplier],
                'Product'[Product],
                'Program'[Program],
                'Program Type'[Program Type],
                'Section'[Section],
                'Size'[Size],
                'Spot Bonus'[Spot Bonus],
                'Spot Match Daypart'[Spot Match Daypart],
                'Spot Match Daypart'[Is Spot Match Prime Time],
                'Spot Product Primary Target'[Spot Product Primary Target],
                'Spot Product Primary Target'[IsCompetitive],
                'Spot Product Primary Target'[IsMasterTarget],
                'Spot Product Secondary Target'[Spot Product Secondary Target],
                'Spot Schedule Primary Target'[Spot Schedule Primary Target],
                'Spot Schedule Secondary Target'[Spot Schedule Secondary Target],
                'Spot Type'[Spot Type],
                'Spot Type'[Spot Type Code],
                'Booking Spot'[ClientRatecardCost],
                'Booking Spot'[AgencyRatecardCost],
                'Booking Spot'[ClientCommissionRebate],
                'Booking Spot'[ClientBaseCost],
                'Booking Spot'[MediaCost],
                'Booking Spot'[MarketCost],
                'Booking Spot'[AgencyCost],
                'Booking Spot'[ServiceFee],
                'Booking Spot'[MonitorFee],
                'Booking Spot'[NominalRate],
                'Booking Spot'[ProductionFee],
                'Booking Spot'[ClientDiscountValue],
                'Booking Spot'[RatecardDiscountValue],
                'Booking Spot'[MediaLevy],
                'Booking Spot'[AgencyCommissionPercentage],
                'Booking Spot'[TaxPercentage],
                'Booking Spot'[VendorDiscount],
                'Booking Spot'[AgencyCommission],
                'Booking Spot'[ClientCost],
                'Booking Spot'[VendorCost],
                'Booking Spot'[RatecardCost],
                'Booking Spot'[RatecardPositionLoadingFee],
                'Booking Spot'[RatecardWeekendLoadingFee],
                'Booking Spot'[RatecardColourLoadingFee],
                'Booking Spot'[ClientPositionLoadingFee],
                'Booking Spot'[ClientWeekendLoadingFee],
                'Booking Spot'[FullPageEquivalentClientCost],
                'Booking Spot'[RatecardBaseCost],
                'Booking Spot'[ClientColourLoadingFee],
                'Booking Spot'[PrintSpotNumberofUnit],
                FILTER(
                    'Booking Spot',
                    [SpotStartDateId] >= {date_from}  && 
                    [SpotStartDateId] <= {date_to} &&
                    [ClientId] in {client_ids}
                )
            )
            """,
        'destination_table_name': 'booking_spot',
        'filters': ['year', 'week']
    },
]
