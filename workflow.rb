require 'rbbt-util'
require 'rbbt/workflow'

Misc.add_libdir if __FILE__ == $0

#require 'rbbt/sources/PerMedCoE'

module PerMedCoE
  extend Workflow

  def self.image_dir
    Rbbt.singularity.find
  end
  
  def self.building_blocks
    Rbbt.modules.BuildingBlocks.find(:lib).glob("*/src/*_BB/")
      .collect{|d| File.basename(d) }
  end

  def self.load_bb_definition(bb)
    dir = Rbbt.modules.BuildingBlocks.glob("*/src/#{bb}").first
    definition = JSON.parse dir["definition.json"].read
    IndiferentHash.setup(definition)
  end

  helper :run_bb do |bb, options|
    definition = PerMedCoE.load_bb_definition(bb)

    positional = options.delete "positional"

    definition["parameters"].each do |name,submode|
      next if positional && positional != name
      submode.each do |info|
        next unless info["type"] == "output"
        Open.mkdir file.output
        outpath = file.output[info["name"]]
        Open.mkdir outpath if info["format"] == 'folder'
        options[info["name"]] = outpath
      end
    end

    general_options = IndiferentHash.setup({})
    options["tmpdir"] ||= file('tmpdir')

    general_options["debug"] = true if config(:debug, :permedcoe, :PerMedCoE, :default => 'false') == 'true'

    new = IndiferentHash.setup({})
    options.each do |k,v|
      v = v.find if Path === v
      v = File.expand_path(v) if String === v && Open.exist?(v)
      new[k] = v
    end
    options = new

    general_options["tmpdir"] = options.delete "tmpdir"
    general_options.delete_if{|k,v| v.nil? }

    Open.mkdir general_options["tmpdir"] if general_options.include? "tmpdir"

    cmd = "env PERMEDCOE_IMAGES=#{PerMedCoE.image_dir} #{bb} "
    cmd << CMD.process_cmd_options(general_options.merge(:add_option_dashes => true)) << " " if general_options.any?
    cmd << positional << " " if positional
    CMD.cmd_log(cmd, options.merge(:add_option_dashes => true))
  end

  def self.load_bb(bb)
    definition = load_bb_definition bb

    desc definition.values_at(:short_description, :long_description).uniq * "\n\n"

    positional = definition["parameters"].keys.uniq

    input :positional, :select, "Positional parameter", positional.first, :select_options => positional  if positional.length > 1

    definition["parameters"].each do |name,submode|
      input_info = {}
      submode.each do |info|
        next unless info["type"] == "input"
        input_description = info["description"]
        input_description = "(#{name}) #{input_description}" if positional.length > 1
        input_info[info["name"]] = [info["format"], input_description]
      end
      input_info.each do |name,p|
        format, input_description = p
        format = 'string' if %w(str).include? format
        format = 'path' if %w(file).include? format
        format = 'path' if %w(folder).include? format
        format = 'integer' if %w(int).include? format
        input name, format, input_description
      end
    end

    task bb => :array do
      input_hash = inputs.to_hash

      new = IndiferentHash.setup({})
      input_hash.each do |k,v|
        v = v.path if Step === v
        v = v.find if Path === v
        new[k] = v
      end
      input_hash = new

      parallel_key = input_hash.keys.select{|k| %w(parallelize parallel).include? k.to_s }.first

      if parallel_key && input_hash[parallel_key].nil?
        input_hash[parallel_key] = config(:cpus, parallel_key, :permedcoe, :PerMedCoE, :default => 1)
      end
      run_bb(self.task_name, input_hash)
      file('output').glob("**/*")
    end
  end

  self.building_blocks.each do |bb|
    load_bb bb
  end

end

#require 'rbbt/knowledge_base/PerMedCoE'
#require 'rbbt/entity/PerMedCoE'

