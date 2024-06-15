import pandas as pd

# Load the data from the 'historical view' sheet
file_path = r''  # Replace with file path
historical_df = pd.read_excel(file_path, 'historical view')

# Filter out cancelled calls
historical_df = historical_df[historical_df['Service/Re&Re/Maintenance Work Order Status'] != 'Cancelled']

# Convert date columns to datetime format
historical_df['Created On'] = pd.to_datetime(historical_df['Created On'])
historical_df['Completed On.'] = pd.to_datetime(historical_df['Completed On.'])

# Replace technician names to combine the profiles
historical_df['Booking Technician 1'] = historical_df['Booking Technician 1'].replace({
    'Azad DO NOT USE THIS PROFILE': 'Azad',
    'Canadian Comfort Home Services Azad': 'Azad'
})

# Sort the data by address and creation date to facilitate the detection of callbacks
historical_df.sort_values(by=['Service Account', 'Created On'], inplace=True)

# Initialize columns to store FCR status, review required flag, callback flag, and redispatched flag
historical_df['FCR Status'] = ''
historical_df['Review Required'] = False
historical_df['Callback'] = False
historical_df['Redispatched'] = False

# Function to determine if two work orders are within 48 hours
def within_48_hours(date1, date2):
    return abs((date1 - date2).total_seconds()) <= 48 * 3600

# Apply the detailed logic to the dataset
def apply_fcr_logic(group):
    group = group.sort_values(by='Created On')
    fcr_status = [''] * len(group)
    review_required = [False] * len(group)
    callback_status = [False] * len(group)
    redispatched_status = [False] * len(group)

    for i in range(len(group)):
        current_completed_on = group.iloc[i]['Completed On.']
        current_contractor = group.iloc[i]['Booking Technician 1']
        print(f"Processing WO {group.iloc[i]['Work Order Number']} for contractor {current_contractor} at {group.iloc[i]['Service Account']}")

        if pd.isnull(current_completed_on):
            continue

        is_callback_within_48_hours = False
        marked_as_callback = False

        for j in range(i + 1, len(group)):
            callback_created_on = group.iloc[j]['Created On']
            callback_contractor = group.iloc[j]['Booking Technician 1']

            print(f"  Comparing with WO {group.iloc[j]['Work Order Number']} for contractor {callback_contractor}")

            # Check if the work order is within 48 hours of completion
            if within_48_hours(current_completed_on, callback_created_on):
                if current_contractor != callback_contractor:
                    fcr_status[i] = 'False'
                    redispatched_status[i] = True
                    print(f"    Marking WO {group.iloc[i]['Work Order Number']} as False and Redispatched due to different contractor within 48 hours")
                fcr_status[j] = ''
                callback_status[j] = False
                is_callback_within_48_hours = True
                break

            # Check if the callback is within 49 hours to 90 days
            if current_completed_on < callback_created_on <= current_completed_on + pd.Timedelta(days=90) and not within_48_hours(current_completed_on, callback_created_on):
                if current_contractor != callback_contractor:
                    fcr_status[i] = 'False'
                    redispatched_status[i] = True
                    print(f"    Marking WO {group.iloc[i]['Work Order Number']} as False and Redispatched due to different contractor within 49 hours to 90 days")
                    break
                else:
                    fcr_status[i] = 'False'
                    print(f"    Marking WO {group.iloc[i]['Work Order Number']} as False due to same contractor callback within 49 hours to 90 days")
                callback_status[j] = True
                marked_as_callback = True
                break

        if not is_callback_within_48_hours and not marked_as_callback:
            if i == len(group) - 1 or group.iloc[i + 1]['Created On'] > current_completed_on + pd.Timedelta(days=90):
                fcr_status[i] = 'True'
                print(f"  Marking WO {group.iloc[i]['Work Order Number']} as True due to no callback within 90 days")

        if i > 0 and group.iloc[i - 1]['Completed On.'] >= current_completed_on - pd.Timedelta(days=90):
            review_required[i] = True
            print(f"  Marking WO {group.iloc[i]['Work Order Number']} for review due to previous completion date overlap")

    for k in range(len(group)):
        if callback_status[k]:
            fcr_status[k] = ''
            print(f"  Marking WO {group.iloc[k]['Work Order Number']} FCR Status as blank due to callback")

    group['FCR Status'] = fcr_status
    group['Review Required'] = review_required
    group['Callback'] = callback_status
    group['Redispatched'] = redispatched_status
    return group

# Apply the FCR logic function to each group
historical_df = historical_df.groupby('Service Account', group_keys=False).apply(apply_fcr_logic)

# Save the updated dataframe to a new Excel file
output_file_path = r''  # Replace with desired file path
historical_df.to_excel(output_file_path, index=False)

print(f'Updated file saved to: {output_file_path}')
