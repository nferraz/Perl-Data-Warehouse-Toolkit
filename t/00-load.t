#!perl -T

use Test::More tests => 4;

BEGIN {
    use_ok('DW::Fact')      || print "Bail out!\n";
    use_ok('DW::Dimesion')  || print "Bail out!\n";
    use_ok('DW::Aggregate') || print "Bail out!\n";
    use_ok('DW::ETL')       || print "Bail out!\n";
}

diag("Testing Perl-Data-Warehouse-Toolkit $DW::Fact::VERSION, Perl $], $^X");
