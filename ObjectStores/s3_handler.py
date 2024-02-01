import ast
import boto3
import logging
import os
import sys
import traceback

LOG_FILE_NAME = 'output.log'

# Change region to match with the default region that you setup when configuring your AWS CLI
REGION = 'us-west-2'

class S3Handler:
    """S3 handler."""

    def __init__(self):
        self.client = boto3.client('s3')

        logging.basicConfig(filename=LOG_FILE_NAME,
                            level=logging.DEBUG, filemode='w',
                            format='%(asctime)s %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p')
        self.logger = logging.getLogger("S3Handler")

    def help(self):
        print("Supported Commands:")
        print("1. createdir <bucket_name>")
        print("2. upload <source_file_name> <bucket_name> [<dest_object_name>]")
        print("3. download <dest_object_name> <bucket_name> [<source_file_name>]")
        print("4. delete <dest_object_name> <bucket_name>")
        print("5. deletedir <bucket_name>")
        print("6. find <pattern> <bucket_name> -- e.g.: find txt bucket1 --")
        print("7. listdir [<bucket_name>]")
    
    def _error_messages(self, issue):
        error_message_dict = {}
        error_message_dict['operation_not_permitted'] = 'Not authorized to access resource.'
        error_message_dict['invalid_directory_name'] = 'Directory name is invalid.'
        error_message_dict['incorrect_parameter_number'] = 'Incorrect number of parameters provided'
        error_message_dict['not_implemented'] = 'Functionality not implemented yet!'
        error_message_dict['bucket_name_exists'] = 'Directory already exists.'
        error_message_dict['bucket_name_empty'] = 'Directory name cannot be empty.'
        error_message_dict['non_empty_bucket'] = 'Directory is not empty.'
        error_message_dict['missing_source_file'] = 'Source file cannot be found.'
        error_message_dict['non_existent_bucket'] = 'Directory does not exist.'
        error_message_dict['non_existent_object'] = 'Destination File does not exist.'
        error_message_dict['unknown_error'] = 'Something was not correct with the request. Try again.'

        if issue:
            return error_message_dict[issue]
        else:
            return error_message_dict['unknown_error']

    def _get_file_extension(self, file_name):
        if os.path.exists(file_name):
            return os.path.splitext(file_name)

    def _get(self, bucket_name):
        response = ''
        try:
            response = self.client.head_bucket(Bucket=bucket_name)
        except Exception as e:
            # print(e)
            # traceback.print_exc(file=sys.stdout)
            
            response_code = e.response['Error']['Code']
            if response_code in ['400', '403', '404']:
                return False
            elif response_code == '200':
                return True
            else:
                raise e
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False

    def createdir(self, bucket_name):
        if not bucket_name:
            return self._error_messages('bucket_name_empty')

        try:
            if self._get(bucket_name):
                return self._error_messages('bucket_name_exists')
            self.client.create_bucket(Bucket=bucket_name,
                                      CreateBucketConfiguration={'LocationConstraint': REGION})
        except Exception as e:
            raise e

        # Success response
        operation_successful = ('Directory %s created.' % bucket_name)
        return operation_successful
    
    def listdir(self, bucket_name=None):
        response = self.client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        
        # If bucket_name is provided,
        if bucket_name:
            # check that bucket exists
            if bucket_name not in buckets:
                return self._error_messages('non_existent_bucket')
            else:
                # display the names of all objects in the bucket
                response = self.client.list_objects(Bucket=bucket_name)
                contents = response.get('Contents')
                if contents is None:
                    return []
                else:
                    return [obj['Key'] for obj in contents]
        # If bucket_name is empty then display the names of all the buckets
        else:
            return buckets

    def upload(self, source_file_name, bucket_name, dest_object_name=None):
        # 1. Parameter Validation
        #    - source_file_name exits in current directory
        #    - bucket_name exists
        if not os.path.exists(source_file_name):
            return self._error_messages('missing_source_file')
        if not self._get(bucket_name):
            return self._error_messages('non_existent_bucket')

        # 2. If dest_object_name is not specified then use the source_file_name as dest_object_name
        if not dest_object_name:
            dest_object_name = source_file_name

        # 3. SDK call
        #    - When uploading the source_file_name and add it to object's meta-data
        self.client.upload_file(source_file_name, bucket_name, dest_object_name)

        # Success response
        # operation_successful = ('File %s uploaded to directory %s.' % (source_file_name, bucket_name))
        return ('File %s uploaded to directory %s.' % (source_file_name, bucket_name))

    def download(self, dest_object_name, bucket_name, source_file_name=None):
        # if source_file_name is not specified then use the dest_object_name as the source_file_name
        # If the current directory already contains a file with source_file_name then move it as a backup
        # with following format: <source_file_name.bak>
        
        # Parameter Validation
        if not self._get(bucket_name):
            return self._error_messages('non_existent_bucket')
        if dest_object_name not in self.listdir(bucket_name):
            return self._error_messages('non_existent_object')
        
        # SDK Call
        if not source_file_name:
            source_file_name = dest_object_name
        if os.path.exists(source_file_name):
            os.rename(source_file_name, source_file_name + '.bak')
        self.client.download_file(bucket_name, dest_object_name, source_file_name)

        # Success response
        # operation_successful = ('File %s downloaded from directory %s.' % (dest_object_name, bucket_name))
        return ('File %s downloaded from directory %s.' % (dest_object_name, bucket_name))

    def delete(self, dest_object_name, bucket_name):
        if not self._get(bucket_name):
            return self._error_messages('non_existent_bucket')
        if dest_object_name not in self.listdir(bucket_name):
            return self._error_messages('non_existent_object')
        
        self.client.delete_object(Bucket=bucket_name, Key=dest_object_name)

        # Success response
        # operation_successful = ('File %s deleted from directory %s.' % (dest_object_name, bucket_name))
        return ('File %s deleted from directory %s.' % (dest_object_name, bucket_name))

    def deletedir(self, bucket_name):
        if not self._get(bucket_name):
            return self._error_messages('non_existent_bucket')
        # Delete the bucket only if it is empty
        if len(self.listdir(bucket_name)) > 0:
            return self._error_messages('non_empty_bucket')
        
        self.client.delete_bucket(Bucket=bucket_name)

        # Success response
        # operation_successful = ("Directory %s deleted." % bucket_name)
        return ("Directory %s deleted." % bucket_name)

    def find(self, pattern, bucket_name=''):
        if not self._get(bucket_name):
            return self._error_messages('non_existent_bucket')
        
        # Return object names that match the given pattern
        return [obj for obj in self.listdir(bucket_name) if pattern in obj]

    def dispatch(self, command_string):
        parts = command_string.split(" ")
        response = ''

        if parts[0] == 'createdir':
            # Figure out bucket_name from command_string
            if len(parts) > 1:
                bucket_name = parts[1]
                response = self.createdir(bucket_name)
            else:
                # Parameter Validation
                # - Bucket name is not empty
                response = self._error_messages('bucket_name_empty')
        elif parts[0] == 'upload':
            # Figure out parameters from command_string
            # source_file_name and bucket_name are compulsory; dest_object_name is optional
            # Use self._error_messages['incorrect_parameter_number'] if number of parameters is less
            # than number of compulsory parameters
            if len(parts) < 3:
                response = self._error_messages('incorrect_parameter_number')
            else:
                response = self.upload(*parts[1:])
        elif parts[0] == 'download':
            # Figure out parameters from command_string
            # dest_object_name and bucket_name are compulsory; source_file_name is optional
            # Use self._error_messages['incorrect_parameter_number'] if number of parameters is less
            # than number of compulsory parameters
            if len(parts) < 3:
                response = self._error_messages('incorrect_parameter_number')
            else:
                response = self.download(*parts[1:])
        elif parts[0] == 'delete':
            if len(parts) < 3:
                response = self._error_messages('incorrect_parameter_number')
            else:
                response = self.delete(*parts[1:])
        elif parts[0] == 'deletedir':
            if len(parts) < 2:
                response = self._error_messages('incorrect_parameter_number')
            else:
                response = self.deletedir(*parts[1:])
        elif parts[0] == 'find':
            if len(parts) < 3:
                response = self._error_messages('incorrect_parameter_number')
            else:
                response = self.find(*parts[1:])
        elif parts[0] == 'listdir':
            response = self.listdir(*parts[1:])
        else:
            response = "Command not recognized."
        return response


def main():

    s3_handler = S3Handler()
    
    while True:
        try:
            command_string = ''
            if sys.version_info[0] < 3:
                command_string = raw_input("Enter command ('help' to see all commands, 'exit' to quit)>")
            else:
                command_string = input("Enter command ('help' to see all commands, 'exit' to quit)>")
    
            # Remove multiple whitespaces, if they exist
            command_string = " ".join(command_string.split())
            
            if command_string == 'exit':
                print("Good bye!")
                exit()
            elif command_string == 'help':
                s3_handler.help()
            else:
                response = s3_handler.dispatch(command_string)
                print(response)
        except Exception as e:
            print(e)

if __name__ == '__main__':
    main()
