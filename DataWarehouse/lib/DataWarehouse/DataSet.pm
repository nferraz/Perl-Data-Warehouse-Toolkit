package DataWarehouse::DataSet;

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
