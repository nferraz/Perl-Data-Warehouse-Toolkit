package DataWarehouse::Transform::Basic;

use warnings;
use strict;

sub head {
	my ($dataset,$n) = @_;

	my $table = $dataset->get_data_rows();

	my @rows = @$table; 
	@rows = splice(@rows,0,$n);		

	$dataset->set_data_rows([ @rows ]);

	return $dataset;
}

sub tail {
	my ($dataset,$n) = @_;

	my $table = $dataset->get_data_rows();
	my $len   = scalar @$table;

	my @rows = @$table; 
	@rows 	 = splice(@rows,$len-$n,$n);		

	$dataset->set_data_rows([ @rows ]);

	return $dataset;
}

sub remove_column {
	my ($dataset,$field_idx) = @_;

	my $header= $dataset->get_headers();
	my $table = $dataset->get_data_rows();
	
	my @rows  = @$table;
	my $index = $field_idx - 1;

	splice(@$header,$index,1);

	my $i = 0;

	foreach my $row (@rows) {
		my (@cols) = @$row;
		splice(@cols,$index,1);

		$rows[$i++] = [ @cols ];	
	}

	$dataset->set_headers($header);
	$dataset->set_data_rows([ @rows ]);

	return $dataset;
}

sub extract_columns {
	my ($dataset,$field_idx) = @_;

	my $header= $dataset->get_headers();
	my $table = $dataset->get_data_rows();
	my @rows  = @$table;

	$header = [ map { $header->[$_-1] } @$field_idx ];

	my $i = 0;

	foreach my $row (@rows) {
		my (@cols)   = @$row;

		my @new_cols = map { $cols[$_-1] } @$field_idx; 
		$rows[$i++]  = [ @new_cols ];
	}

	$dataset->set_headers($header);
	$dataset->set_data_rows([ @rows ]);

	return $dataset;
}

sub add_rownum {
	my $dataset = shift;

	my $header= $dataset->get_headers();
	my $table = $dataset->get_data_rows();

	my @rows  = @$table;
	my @new_rows;

	my $i = 0;

	foreach my $row (@rows) {
		my @cols = (@$row,++$i);
		push @new_rows, [ @cols ];	
	}

	push @$header,"Sequence column";
	$dataset->set_headers($header);
	$dataset->set_data_rows([ @new_rows ]);

	return $dataset;
}

sub add_computed_field {
	my ($dataset,$sub_ref,$head) = @_;

	my $header= $dataset->get_headers();
	my $table = $dataset->get_data_rows();

	my @rows = @$table;
	my $i 	 = 0;

	foreach (@rows) {
		$sub_ref->($_);
	}

	push @$header,$head;
	$dataset->set_headers($header);
	$dataset->set_data_rows([ @rows ]);

	return $dataset;
}

1;

__END__

=head1 NAME

DataWarehouse::Transform::Basic - module providing transformation operations on data sets

=head1 VERSION

Version 0.01

=head1 SYNOPSIS

my $rows = [
                [ 1, 4, 6 ],
                [ 3, 2, 7 ],
           ];

my @head =  DataWarehouse::Transform::Basic::head($rows,1);

my @tail =  DataWarehouse::Transform::Basic::tail($rows,1);

my @rows =  DataWarehouse::Transform::Basic::remove_column($rows,2);

my @rows1 = DataWarehouse::Transform::Basic::extract_columns($rows,[2]);

my @rows2 = DataWarehouse::Transform::Basic::add_rownum($rows);

=head1 DESCRIPTION

This module provides various transformations to be performed on any data-set. 

=head1 FUNCTIONS

=over

=item 

head($dataset,$n)

Parameters: Data-set object 

Return value: Modified DataSet object with first n rows 

=cut

=item

tail($dataset,$n)

Parameters: Data-set object 

Return value: Modified DataSet object with last n rows 

=cut

=item

remove_column($dataset,$n)

Parameters: Data-set object 

Return value: Modified DataSet object with n-th column removed in data table 

=cut

=item

extract_columns($dataset,$fields_idx_array_ref);

Parameters: Data-set object 

Return value: Modified DataSet object containing only the fields corresponding to the indices in data table

=cut

=item

add_rownum($dataset)

Parameters: Data-set object 

Return value: Modified DataSet object with additional numeric auto-inc column in data table

=cut

=item

add_computed_field($dataset,$sub_ref,$head)

Parameters: Data-set object, subroutine reference, String heading for computed field

Return value: Modified DataSet object

=back

=head1 AUTHOR

Ashish Mukherjee (ashish.mukherjee@gmail.com) 
