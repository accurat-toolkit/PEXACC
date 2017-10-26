# PEXACC configuration file. Change this before running!
#
# (C) ICIA 2011, Radu ION.
#
# ver 1.0, 22.09.2011, Radu ION: Windows/Unix portable.
# ver 1.1, 04.10.2011, Radu ION: added file manipulations functions.
# ver 2.0, 11.10.2011, Radu ION: corresponding to PEXACC2.
# ver 2.01, 3.11.2011, Radu ION: added selective debug messsages.
# ver 3.0, 23.11.2011, Radu ION: heavy modifications: no NFS, scp between master and worker, all files copied on each cluster node.
# ver 4.0, 15.12.2001, Radu ION: S2T and T2S dictionaries for symmetrical measure.

package pexacc2conf;

use strict;
use warnings;
use File::Spec;
use File::Path;
use Sys::Hostname;

sub checkDir( $$ );
sub checkFile( $$ );
sub checkSplitMode( $$ );
sub checkWeights( $$$ );
sub checkClusterFile( $$ );
sub checkInt( $$ );
sub checkProb( $$ );
sub checkReal( $$ );
sub checkBool( $$ );
sub checkLang( $$ );
sub checkIP( $$ );

sub new;
sub addValue( $$$$ );
sub genClusterFile();
sub findMyIPAdress();
sub portableCopyFile2File( $$ );
sub portableCopyFileToDir( $$ );
sub portableRemoveFile( $ );
sub portableRemoveFileFromDir( $$ );
sub portableRemoveAllFilesFromDir( $ );
sub portableForkAndDetach( $ );
sub portableVerboseSystem( $ );
sub portableListFiles( $ );
sub portableRemoteCopy( $$$$ );

##################
#CONFIG FILE######
##################
my( $DEBUG ) = 0;

#Only change values between 'BEGIN CONF' and 'END CONF'!
sub new {
	my( $classname ) = shift();
	my( $conf ) = shift();
	my( $this ) = {};
	
	############################## BEGIN CONF ##############################################################################
	#MODIFY THE LAST ARGUMENT OF THE addvalue() function.
	#Possible values: strings, integers, booleans, real numbers.

	#Source language
		#IN: --source LANG
	addValue( $this, $conf, "SRCL", "en" );
	#Target language
		#IN: --target LANG
	addValue( $this, $conf, "TRGL", "ro" );
	#MUST EXIST!
	#This is the directory containing 'res' and 'dict' resources directories on worker machines.
	#This is the directory which contains the aligned documents on the master machine.
	#Alignments of the documents are relative to PEXACCWORKINGDIR
	#This directory MUST HAVE THE SAME PATH on master and worker machines!!
	#MUST BE AN ABSOLUTE PATH!
	#Windows/Unix Not OK! SET!
	addValue( $this, $conf, "PEXACCWORKINGDIR", "." );
	#Master IP. This is the IP of the machine that runs 'pexacc2.pl'.
	#For clustering purposes THIS MAY NOT BE '127.0.0.1'!
	#If not doing clustering, you may set this to '127.0.0.1'.
	#A value of 'autodetect' will attempt to discover this address but this method is not very reliable.
	#Windows/Unix Not OK! SET!
	addValue( $this, $conf, "MASTERIP", "127.0.0.1" );
	#Windows/Unix OK!
	#MUST BE relative to PEXACCWORKINGDIR!
	addValue( $this, $conf, "GIZAPPNEWDICTDIR", File::Spec->catdir( $this->{"PEXACCWORKINGDIR"}, "dict", "learntdict" ) );
	#The GIZA++ executable.
	#PLEASE CHANGE THAT TO MATCH YOUR INSTALLATION!
	#Windows/Unix Not OK! SET!
		#IN: --param GIZAPPEXE=/path/to/GIZA++
	addValue( $this, $conf, "GIZAPPEXE", "/usr/local/giza++-1.0.5/bin/GIZA++.exe" );
	#The utility to convert from plain text to GIZA++ format.
	#PLEASE CHANGE THAT TO MATCH YOUR INSTALLATION!
	#Windows/Unix Not OK! SET!
		#IN: --param PLAIN2SNTEXE=/path/to/plain2snt.out
	addValue( $this, $conf, "PLAIN2SNTEXE", "/usr/local/giza++-1.0.5/bin/plain2snt.exe" );
	#GIZA++ configuration file
	#This file will be automatically updated by pdataextract-p.pl so make sure that it's writable!
	#Make sure it's in the same directory as pdataextract-p.pl!
	#GIZA++ documentation gives additional information on this file if one wishes to play with GIZA++ parameters.
	#If not given, it will be read from the current directory.
	#Windows/Unix OK!
	addValue( $this, $conf, "GIZAPPCONF", File::Spec->catdir( ".", "pdataextract-gizapp.gizacfg" ) );

	#Local mount point. Intermediary files are kept in this dir. If not given, it will be created (on all cluster nodes).
	#Windows/Unix OK!
	my( $tmpdir ) = File::Spec->catdir( File::Spec->tmpdir(), "pdex" );
	
	mkpath( $tmpdir );
	addValue( $this, $conf, "TMPDIR", $tmpdir );

	#Corpus name (going to be in the name of the output file).
	#Change it to whatever corpus you are processing: Sheffield, Wikipedia, etc. :)
	addValue( $this, $conf, "CORPUSNAME", "pexacc2-run" );
	#Out file basename for extracted parallel phrases.
	#The output file will be placed in the same directory as the pdataextract-p.pl resides.
	#The iteration number and .txt extension will be added to this basename.
	#Best to be left unchanged.
	addValue( $this, $conf, "OUTFILEBN", $this->{"SRCL"} . "-" . $this->{"TRGL"} . "-" . $this->{"CORPUSNAME"} . "-pexacc2" );
	#If this is specified, the last output file (named using OUTFILEBN and last iteration number) will take this name.
		#IN: --output FILE
	addValue( $this, $conf, "OUTFILE", "" );
	#The cluster file. Number of processors on each machine.
	#If the value is 'generate' a './cluster.info' file will be automatically generated in the current directory.
		#IN: --param CLUSTERFILE=FILE|generate
	addValue( $this, $conf, "CLUSTERFILE", "generate" );
	#Use translation equivalents with at least this probability from the base (main) GIZA++ dictionary...
	addValue( $this, $conf, "GIZAPPTHR", 0.001 );
	#From the new-learnt dictionaries, use translation equivalents with at least this probability...
	addValue( $this, $conf, "NEWGIZAPPTHR", 0.1 );
	#Sure GIZA++ probability threshold (translation equivalents with at lest this probability are considered correct)
	addValue( $this, $conf, "SUREGIZAPPTHR", 0.33 );
	#Indentical strings in source and target language are not allowed.
	#So reject a pair of phrases if they are more similar than...
	addValue( $this, $conf, "IDENTICALPHRTHR", 0.99 );
	#Source/Target sentence/chunk ratio in words (biggest/smallest) (trained: 1.5 for en-ro)
		#IN: --param SENTRATIO=1.5
	addValue( $this, $conf, "SENTRATIO", 1.5 );

	#Windows/Unix OK!
	#MUST BE relative to PEXACCWORKINGDIR!
	#Source-target (PEXACC similarity measure is symmetrical)
	my( $DICTFILEST ) = File::Spec->catfile( $this->{"PEXACCWORKINGDIR"}, "dict", $this->{"SRCL"} . "_" . $this->{"TRGL"} );
	#Target-source
	my( $DICTFILETS ) = File::Spec->catfile( $this->{"PEXACCWORKINGDIR"}, "dict", $this->{"TRGL"} . "_" . $this->{"SRCL"} );
	
	#The main dictionary file.
	addValue( $this, $conf, "DICTFILEST", $DICTFILEST );
	addValue( $this, $conf, "DICTFILETS", $DICTFILETS );

	#The weights with which to combine the probabilities of the main and learnt dictionaries (must sum to 1):
	addValue( $this, $conf, "DICTWEIGHTMAIN", 0.7 );
	addValue( $this, $conf, "DICTWEIGHTLEARNT", 0.3 );
	#Learnt dictionary file:
	#Windows/Unix OK!
	addValue( $this, $conf, "LEARNTDICTFILEST", File::Spec->catfile( $this->{"GIZAPPNEWDICTDIR"}, $this->{"SRCL"} . "-" . $this->{"TRGL"} . "-GIZA++-" . $this->{"CORPUSNAME"} . ".gpp" ) );
	addValue( $this, $conf, "LEARNTDICTFILETS", File::Spec->catfile( $this->{"GIZAPPNEWDICTDIR"}, $this->{"TRGL"} . "-" . $this->{"SRCL"} . "-GIZA++-" . $this->{"CORPUSNAME"} . ".gpp" ) );
	
	#Resources. Make sure that these files from the PEXACC kit are installed in the right places and readable.
	#MUST BE relative to PEXACCWORKINGDIR!
	#Windows/Unix OK!
	addValue( $this, $conf, "ENMARKERSFILE", File::Spec->catfile( $this->{"PEXACCWORKINGDIR"}, "res", "markers-" . $this->{"SRCL"} . ".txt" ) );
	addValue( $this, $conf, "ROMARKERSFILE", File::Spec->catfile( $this->{"PEXACCWORKINGDIR"}, "res", "markers-" . $this->{"TRGL"} . ".txt" ) );
	addValue( $this, $conf, "ENSTOPWORDSFILE", File::Spec->catfile( $this->{"PEXACCWORKINGDIR"}, "res", "stopwords_" . $this->{"SRCL"} . ".txt" ) );
	addValue( $this, $conf, "ROSTOPWORDSFILE", File::Spec->catfile( $this->{"PEXACCWORKINGDIR"}, "res", "stopwords_" . $this->{"TRGL"} . ".txt" ) );
	addValue( $this, $conf, "INFLENFILE", File::Spec->catfile( $this->{"PEXACCWORKINGDIR"}, "res", "endings_" . $this->{"SRCL"} . ".txt" ) );
	addValue( $this, $conf, "INFLROFILE", File::Spec->catfile( $this->{"PEXACCWORKINGDIR"}, "res", "endings_" . $this->{"TRGL"} . ".txt" ) );
	#Do lemmatization or not? (0 or 1)
	addValue( $this, $conf, "LEMMAS", 1 );
	#Split mode may be 'sent' from split text @ sentence boundaries or 'chunk' from split sentences @ marker level.
		#IN: --param SPLITMODE={chunk|sent}
	addValue( $this, $conf, "SPLITMODE", "chunk" );
	#Output threshold for pairs of "parallel" (as determined by algorithm) phrases (values between 0 and 1 or 0..1):
		#IN: --param OUTPUTTHR=0.2
	addValue( $this, $conf, "OUTPUTTHR", 0.2 );
	#GIZA++ dictionary training is done on pairs of phrases having at least this parallelism threshold (0..1):
	#This value is different for sent (0.3) and chunk (0.5)
	addValue( $this, $conf, "GIZAPPPARALLELTHR", 0.3 );
	#How many bootstrapping iterations (extract parallel phrases, extract GIZA++ dicts and reloop):
		#IN: --param GIZAPPITERATIONS=3
	addValue( $this, $conf, "GIZAPPITERATIONS", 3 );
	#Cognates: similarity threshold between a source and a target word for them to be considered cognates.
	addValue( $this, $conf, "SSTHR", 0.7 );
	#How apart are phrases (in absolute positions from the beginning of the document) such that they are considered "adjacent".
	#7
	#0 disables the feature
	addValue( $this, $conf, "CLUSTERLIM", 0 );
	#DEBUG messages or not (bool)
	addValue( $this, $conf, "DEBUG", 1 );
	#This is a bool indicating the status of remote computing
	#This value is dynamically set by PEXACC2
	addValue( $this, $conf, "REMOTEWORKER", 0 );
	
	$DEBUG = $this->{"DEBUG"};
	############################## END CONF ################################################################################
	
	checkLang( "SRCL", $this );
	checkLang( "TRGL", $this );
	checkDir( "PEXACCWORKINGDIR", $this );
	checkDir( "TMPDIR", $this );
	checkIP( "MASTERIP", $this );
	checkDir( "GIZAPPNEWDICTDIR", $this );
	checkClusterFile( "CLUSTERFILE", $this );
	checkFile( "GIZAPPCONF", $this );
	checkFile( "GIZAPPEXE", $this );
	checkFile( "DICTFILEST", $this );
	checkFile( "DICTFILETS", $this );
	checkFile( "ENMARKERSFILE", $this );
	checkFile( "ROMARKERSFILE", $this );
	checkSplitMode( "SPLITMODE", $this );
	checkReal( "OUTPUTTHR", $this );
	checkReal( "SENTRATIO", $this );
	checkBool( "LEMMAS", $this );
	checkBool( "DEBUG", $this );
	checkBool( "REMOTEWORKER", $this );
	checkProb( "SSTHR", $this );
	checkProb( "GIZAPPTHR", $this );
	checkProb( "IDENTICALPHRTHR", $this );
	checkProb( "DICTWEIGHTMAIN", $this );
	checkProb( "DICTWEIGHTLEARNT", $this );
	checkWeights( "DICTWEIGHTMAIN + DICTWEIGHTLEARNT", $this->{"DICTWEIGHTMAIN"}, $this->{"DICTWEIGHTLEARNT"} );
	checkInt( "GIZAPPITERATIONS", $this );
	checkInt( "CLUSTERLIM", $this );
	checkReal( "GIZAPPPARALLELTHR", $this );
	
	bless( $this, $classname );
	return $this;
}

#The rest of these functions are not to be called through the object interface.
sub addValue( $$$$ ) {
	my( $this, $conf, $varname, $vardefaultvalue ) = @_;
	
	if ( exists( $conf->{$varname} ) && $conf->{$varname} ne "" ) {
		$this->{$varname} = $conf->{$varname};
	}
	else {
		$this->{$varname} = $vardefaultvalue;
	}
}

sub checkIP( $$ ) {
	my( $varname, $this ) = @_;
	my( $ip ) = $this->{$varname};
	
	if ( $ip =~ /autodetect/i ) {
		$this->{$varname} = portableFindMyIPAdress();
	}
	elsif ( $ip !~ /^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$/ ) {
		die( "pexacc2conf::checkIP: '$varname' is not a valid IP address !\n" );
	}
	else {
		my( @ipbytes ) = ( $ip =~ /^([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})$/ );
		
		foreach my $b ( @ipbytes ) {
			if ( $b > 255 ) {
				die( "pexacc2conf::checkIP: '$varname' is not a valid IP address !\n" );
			}
		}
	}
}

sub checkDir( $$ ) {
	my( $varname, $this ) = @_;
	my( $dir ) = $this->{$varname};
	my( $testfile ) = File::Spec->catfile( $dir, "test245blah790" );
	
	open( TF, ">", $testfile ) or die( "pexacc2conf::checkDir: '$varname' has issues: '$! (" . int( $! ) . ")'.\n" );
	close( TF );

	unlink( $testfile ) or warn( "pexacc2conf::checkDir: could not remove '$testfile' because '$!'.\n" );
}

sub checkClusterFile( $$ ) {
	my( $varname, $this ) = @_;
	
	if ( $this->{$varname} eq "generate" ) {
		genClusterFile();
		$this->{$varname} = "cluster-autogen.info";
	}
	
	checkFile( $varname, $this );
}

sub checkFile( $$ ) {
	my( $varname, $this ) = @_;
	my( $file ) = $this->{$varname};
	
	if ( ! -f ( $file ) ) {
		die( "pexacc2conf::checkFile: '$varname' does not exist !\n" );
	} 
}

sub checkSplitMode( $$ ) {
	my( $varname, $this ) = @_;
	my( $smode ) = $this->{$varname};
	
	if ( $smode ne "sent" && $smode ne "chunk" ) {
		die( "pexacc2conf::checkSplitMode: invalid value for '$varname' (either 'chunk' or 'sent') !\n" );
	}
}

sub checkWeights( $$$ ) {
	my( $varname, $w1, $w2 ) = @_;
	
	if ( ( $w1 + $w2 ) != 1 ) {
		die( "pexacc2conf::checkWeights: invalid value for '$varname' (must sum to 1) !\n" );
	}
}

sub checkInt( $$ ) {
	my( $varname, $this ) = @_;
	my( $int ) = $this->{$varname};
	
	if ( $int !~ /^[0-9]+$/ ) {
		die( "pexacc2conf::checkInt: invalid value for '$varname' !\n" );
	}
}

sub checkProb( $$ ) {
	my( $varname, $this ) = @_;
	my( $prob ) = $this->{$varname};
	
	if ( $prob !~ /^[0-9]+(?:\.[0-9]+)?(?:[eE]-?[0-9]+)?$/ ) {
		die( "pdataextractconf::checkProb: invalid value for '$varname' (real number) !\n" );
	}
	
	if ( $prob < 0 || $prob > 1 ) {
		die( "pexacc2conf::checkProb: invalid value for '$varname' ([0..1]) !\n" );
	}	
}

sub checkReal( $$ ) {
	my( $varname, $this ) = @_;
	my( $real ) = $this->{$varname};
	
	if ( $real !~ /^[0-9]+(?:\.[0-9]+)?(?:[eE]-?[0-9]+)?$/ ) {
		die( "pexacc2conf::checkReal: invalid value for '$varname' !\n" );
	}
}

sub checkBool( $$ ) {
	my( $varname, $this ) = @_;
	my( $bool ) = $this->{$varname};
	
	if ( $bool !~ /^[01]$/ ) {
		die( "pexacc2conf::checkBool: invalid value for '$varname' (either '0' or '1') !\n" );
	}
}

sub checkLang( $$ ) {
	my( $varname, $this ) = @_;
	my( $lang ) = $this->{$varname};
	
	if ( $lang !~ /^(?:en|ro|de|lt|lv|sl|el|hr|et)$/ ) {
		die( "pdataextractconf::checkLang: invalid value for '$varname' !\n" );
	}
}

sub genClusterFile() {
	#Windows/Linux OK!
	my( $thishostname ) = hostname();
	
	open( CLF, ">", "cluster-autogen.info" ) or die( "pdataextractconf::genClusterFile: cannot open file 'cluster-autogen.info' !\n" );
	
	print( CLF "#This is a comment.\n" );
	print( CLF "#This autogenerated file will NOT work if a cluster run is desired!\n" );
	print( CLF "#Line format (tab separated fields):\n" );
	print( CLF "#- hostname of the machine in cluster (run 'hostname' command)\n" );
	print( CLF "#- IP of the machine\n" );
	print( CLF "#- ID (string) of one CPU core\n\n" );
	
	#Linux systems...
	if ( -f ( "/proc/cpuinfo" ) ) {
		open( CPU, "<", "/proc/cpuinfo" ) or die( "pdataextractconf::genClusterFile: cannot open file '/proc/cpuinfo' !\n" );
		
		while ( my $line = <CPU> ) {
			$line =~ s/^\s+//;
			$line =~ s/\s+$//;
			
			next if ( $line !~ /:/ );
			
			my( $variable, $value ) = split( /\s*:\s*/, $line );
			
			$variable =~ s/^\s+//;
			$variable =~ s/\s+$//;
			$value =~ s/^\s+//;
			$value =~ s/\s+$//;
			
			if ( $variable eq "processor" ) {
				print( CLF $thishostname . "\t" . "127.0.0.1" . "\t" . "cpu$value" . "\n" );
			}
		}
		
		close( CPU );
	} 
	#Windows or other systems...
	else {
		#Don't know. 1 core :D
		print( CLF $thishostname . "\t" . "127.0.0.1" . "\t" . "cpu0" . "\n" );
	}
	
	close( CLF );
}

sub portableCopyFile2File( $$ ) {
	my( $filea, $fileb ) = @_;

	#Windows run
	if ( $^O =~ /^MSWin(?:32|64)$/i ) {
		if ( $DEBUG ) {
			warn( "`copy \/Y ${filea} ${fileb}'\n" );
		}
		
		qx/copy \/Y ${filea} ${fileb}\\/;
	}
	#Linux run
	elsif ( $^O =~ /^Linux$/i || $^O =~ /^Cygwin$/i || $^O =~ /^MSys$/i ) {
		if ( $DEBUG ) {
			qx/cp -fv ${filea} ${fileb} 1>&2/;
		}
		else {
			qx/cp -f ${filea} ${fileb}/;
		}
	}
	else {
		die( "pexacc2conf::portableRenameFile: unsupported operating system '$^O' !\n" );
	}
}

sub portableCopyFileToDir( $$ ) {
	my( $file, $dir ) = @_;

	#Windows run
	if ( $^O =~ /^MSWin(?:32|64)$/i ) {
		if ( $DEBUG ) { 
			warn( "`copy \/Y ${file} ${dir}\\'\n" );
		}
		
		qx/copy \/Y ${file} ${dir}\\/;
	}
	#Linux run
	elsif ( $^O =~ /^Linux$/i || $^O =~ /^Cygwin$/i || $^O =~ /^MSys$/i ) {
		if ( $DEBUG ) {
			qx/cp -fv ${file} ${dir}\/ 1>&2/;
		}
		else {
			qx/cp -f ${file} ${dir}\//;
		}
	}
	else {
		die( "pexacc2conf::portableCopyFileToDir: unsupported operating system '$^O' !\n" );
	}
}

sub portableRemoveFile( $ ) {
	my( $file ) = $_[0];

	#Windows run
	if ( $^O =~ /^MSWin(?:32|64)$/i ) {
		if ( $DEBUG ) {
			warn( "`del \/F \/Q ${file}'\n" );
		}
		
		qx/del \/F \/Q ${file}/;
	}
	#Linux run
	elsif ( $^O =~ /^Linux$/i || $^O =~ /^Cygwin$/i || $^O =~ /^MSys$/i ) {
		if ( $DEBUG ) {
			qx/rm -fv ${file} 1>&2/;
		}
		else {
			qx/rm -f ${file}/;
		}
	}
	else {
		die( "pexacc2conf::portableRemoveFile: unsupported operating system '$^O' !\n" );
	}
}

sub portableRemoveFileFromDir( $$ ) {
	my( $dir, $file ) = @_;

	#Windows run
	if ( $^O =~ /^MSWin(?:32|64)$/i ) {
		if ( $DEBUG ) {
			warn( "`del \/F \/Q ${dir}\\${file}'\n" );
		}
		
		qx/del \/F \/Q ${dir}\\${file}/;
	}
	#Linux run
	elsif ( $^O =~ /^Linux$/i || $^O =~ /^Cygwin$/i || $^O =~ /^MSys$/i ) {
		if ( $DEBUG ) {
			qx/rm -fv ${dir}\/${file} 1>&2/;
		}
		else {
			qx/rm -f ${dir}\/${file}/;
		}
	}
	else {
		die( "pexacc2conf::portableRemoveFileFromDir: unsupported operating system '$^O' !\n" );
	}
}

sub portableRemoveAllFilesFromDir( $ ) {
	my( $dir ) = $_[0];

	#Windows run
	if ( $^O =~ /^MSWin(?:32|64)$/i ) {
		if ( $DEBUG ) {
			warn( "`/del \/F \/Q ${dir}\\'\n" );
		}
		
		qx/del \/F \/Q ${dir}\\/;
	}
	#Linux run
	elsif ( $^O =~ /^Linux$/i || $^O =~ /^Cygwin$/i || $^O =~ /^MSys$/i ) {
		if ( $DEBUG ) {
			qx/rm -fv ${dir}\/* 1>&2/;
		}
		else {
			qx/rm -f ${dir}\/*/;
		}
	}
	else {
		die( "pexacc2conf::portableRemoveAllFilesFromDir: unsupported operating system '$^O' !\n" );
	}
}

sub portableForkAndDetach( $ ) {
	my( $cmd ) = $_[0];

	#Windows run
	if ( $^O =~ /^MSWin(?:32|64)$/i ) {
		if ( $DEBUG ) {
			warn( "`start /B ${cmd}'\n" );
		}
		
		system( "start /B ${cmd}" );
	}
	#Linux run
	elsif ( $^O =~ /^Linux$/i || $^O =~ /^Cygwin$/i || $^O =~ /^MSys$/i ) {
		if ( $DEBUG ) {
			warn( "`${cmd} &'\n" );
		}
		
		system( "${cmd} &" );
	}
	else {
		die( "pexacc2conf::portableForkAndDetach: unsupported operating system '$^O' !\n" );
	}
}

sub portableVerboseSystem( $ ) {
	my( $cmd ) = $_[0];

	#Windows run
	if ( $^O =~ /^MSWin(?:32|64)$/i ) {
		if ( $DEBUG ) {
			warn( "`${cmd}'\n" );
		}
		
		system( "${cmd}" );
	}
	#Linux run
	elsif ( $^O =~ /^Linux$/i || $^O =~ /^Cygwin$/i || $^O =~ /^MSys$/i ) {
		if ( $DEBUG ) {
			warn( "`${cmd} 1>&2'\n" );
		}
		
		system( "${cmd} 1>&2" );
	}
	else {
		die( "pexacc2conf::verboseSystem: unsupported operating system '$^O' !\n" );
	}
}

sub portableListFiles( $ ) {
	my( $dirwithmask ) = $_[0];

	#Windows run
	if ( $^O =~ /^MSWin(?:32|64)$/i ) {
		my( @files ) = qx/dir \/B ${dirwithmask}/;
		
		foreach my $f ( @files ) {
			$f =~ s/^\s+//;
			$f =~ s/\s+$//;
		}
		
		return @files;
	}
	#Linux run
	elsif ( $^O =~ /^Linux$/i || $^O =~ /^Cygwin$/i || $^O =~ /^MSys$/i ) {
		my( @files ) = qx/ls -1 ${dirwithmask} 2>\/dev\/null/;
		
		foreach my $f ( @files ) {
			$f =~ s/^\s+//;
			$f =~ s/\s+$//;
		}
		
		return @files;
	}
	else {
		die( "pexacc2conf::verboseSystem: unsupported operating system '$^O' !\n" );
	}
} 

sub portableRemoteCopy( $$$$ ) {
	my( $localfile, $remoteuser, $remotemachine, $remotedir ) = @_;

	if ( $^O =~ /^Linux$/i || $^O =~ /^Cygwin$/i || $^O =~ /^MSys$/i ) {
		if ( $DEBUG ) {
			qx/scp ${localfile} ${remoteuser}\@${remotemachine}:${remotedir} 1>&2/;
		}
		else {
			qx/scp -q ${localfile} ${remoteuser}\@${remotemachine}:${remotedir}/;
		}
	}
	else {
		die( "pexacc2conf::portableRemoteCopy: unsupported operating system '$^O' !\n" );
	}
}

sub portableFindMyIPAdress() {
	my( $myip ) = "127.0.0.1";

	#Windows run
	if ( $^O =~ /^MSWin(?:32|64)$/i ) {
		my( @output ) = qx/ipconfig/;
		my( $outstring ) = join( "", @output );
		#IPv4 Address. . . . . . . . . . . : 89.38.230.4
		my( @allips ) = ( $outstring =~ /IPv4\s+Address[\s.]+:\s*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})/g );
		
		if ( scalar( @allips ) > 1 ) {
			die( "pexacc2conf::portableFindMyIPAdress: multiple IPs detected! Please choose from: " . join( ", ", @allips ) . "\n" );
		}
		elsif ( scalar( @allips ) == 1 ) {
			$myip = $allips[0];
		}
		else {
			die( "pexacc2conf::portableFindMyIPAdress: no IP(s) detected! Will set 127.0.0.1 ...\n" );
		}
	}
	#Linux run
	elsif ( $^O =~ /^Linux$/i || $^O =~ /^Cygwin$/i || $^O =~ /^MSys$/i ) {
		my( @output ) = qx/ifconfig/;
		my( $outstring ) = join( "", @output );
		#inet addr:172.16.39.117
		my( @allips ) = ( $outstring =~ /inet\s+addr\s*:\s*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})/g );
		
		if ( scalar( @allips ) > 1 ) {
			die( "pexacc2conf::portableFindMyIPAdress: multiple IPs detected! Please choose from: " . join( ", ", @allips ) . "\n" );
		}
		elsif ( scalar( @allips ) == 1 ) {
			$myip = $allips[0];
		}
		else {
			die( "pexacc2conf::portableFindMyIPAdress: no IP(s) detected! Will set 127.0.0.1 ...\n" );
		}		
	}
	else {
		die( "pexacc2conf::portableFindMyIPAdress: unsupported operating system '$^O' !\n" );
	}

	return $myip;
}

1;
