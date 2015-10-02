require 'pg'
conn = PG::Connection.open(:dbname => 'SmartStats_development')
conn.prepare('insertVolume', 'insert into volume values($1, $2)')
File.open("file", "r") do |f|
f.each_line do |line|
    date = line[/(\d+)\s+(\d+)/, 1]
    volume = line[/(\d+)\s+(\d+)/, 2]
res = conn.exec_prepared('insertVolume', [date, volume.to_i]) 
 end
end
