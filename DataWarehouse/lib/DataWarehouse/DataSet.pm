package DataWarehouse::DataSet;

use strict;
use fields qw(rows headers);

sub new {
	my $class = shift;
	my ($args)  = @_;

	my $self = {
		'rows'    => $args->{rows}    || [],
		'headers' => $args->{headers} || [],	
	};

	bless $self, $class;
}

sub set_data_rows {
	my ($self,$table) = @_;

	$self->{rows} = $table;
}

sub set_headers {
	my ($self,$headers) = @_;
		
	$self->{headers} = $headers;
}

sub get_data_rows {
	my $self = shift;
	return $self->{rows};	
}

sub get_headers {
	my $self = shift;
	return $self->{headers};
}

1;

__END__

=head1 NAME

DataWarehouse::DataSet - class for an ETL dataset

=head1 VERSION

Version 0.01

=head1 SYNOPSIS

my $rows = [
                [ 1, 4 ],
                [ 3, 2 ],
                [ 3, 2 ],
           ];


my $headers = [ 'heading1', 'heading2' ];

my $dataset = new DataWarehouse::DataSet({ rows => $rows, headers => $headers });

$dataset->set_headers($headers); 

$dataset->set_data_rows($rows);

=head1 DESCRIPTION

This module encapsulates data rows and meta-data for an ETL dataset

=head1 FUNCTIONS

=over

=item

Parameters: Pass reference to array of colums heads

set_headers($headers)

=cut

=item

Parameters: Pass reference to array of data rows 

set_data_rows($rows)

=cut

=item

get_headers()

Returns reference to array of heads

=cut

=item

get_data_rows()

Returns reference to array of rows

=cut

=back

=head1 AUTHOR

Ashish Mukherjee (ashish.mukherjee@gmail.com) 
