# ver 0.1, Radu ION, 15.12.2011: created. Implements the 0-1F range Levenshtein distance on two strings. Will use it if String::Similarity is not available.

package strsim;

use strict;
use warnings;

my( $SSOK ) = 1;

BEGIN {
	eval "use String::Similarity ();";
	
	$SSOK = 0 if ( $@ );
}

sub similarity( $$ );
sub min( $$ );
sub max( $$ );

sub min( $$ ) {
	my( $x, $y ) = @_;
	
	return ( $x <= $y ? $x : $y );
}

sub max( $$ ) {
	my( $x, $y ) = @_;
	
	return ( $x >= $y ? $x : $y );
}

#Levenshtein Distance implementation.
sub similarity( $$ ) {
	my( $s, $t ) = @_;
	
	#String::Similarity implementation
	if ( $SSOK ) {
		return String::Similarity::similarity( $s, $t );
	}
	
	my( @s ) = split( //, $s );
	my( @t ) = split( //, $t );
	my( $ret ) = 0;

	if ( ! defined( $s ) || $s eq "" || ! defined( $t ) || $t eq "" ) {
		return 0;
	}

	my( $n ) = length( $s ); # length of s
	my( $m ) = length( $t ); # length of t
	my( @p ) = (); # 'previous' cost array, horizontally
	my( @d ) = (); # cost array, horizontally
	my( @_d ) = (); # placeholder to assist in swapping p and d

	# indexes into strings s and t
	my( $i ) = 0; # iterates through s
	my( $j ) = 0; # iterates through t
	my( $t_j ) = ''; # jth character of t
	my( $cost ) = 0; # cost

	for ( $i = 0; $i <= $n; $i++ ) {
		$p[$i] = $i;
	}

	for ( $j = 1; $j <= $m; $j++ ) {
		$t_j = $t[$j - 1];
		$d[0] = $j;

		for ( $i = 1; $i <= $n; $i++ ) {
			$cost = ( ( $s[$i - 1] eq $t_j ) ? 0 : 1 );
				
			# minimum of cell to the left+1, to the top+1, diagonally
			# left
			# and up +cost
			$d[$i] = min( min( $d[$i - 1] + 1, $p[$i] + 1 ), $p[$i - 1] + $cost );
		} # end i

		# copy current distance counts to 'previous row' distance
		# counts
		@_d = @p;
		@p = @d;
		@d = @_d;
	} # end j

	# our last action in the above loop was to switch d and p, so p now
	# actually has the most recent cost counts
	$ret = $p[$n];

	return ( max( $n, $m ) - $ret ) / max( $n, $m );
} # similarity( string s, string t )



1;
